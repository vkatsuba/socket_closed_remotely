-module(myapp_server).
-behavior(gen_server).

-define(KNOWN_RESPONSE_BODY_BYTES, 2000000).

-export([    
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).

-record(state, {
    host,
    counter,
    client,
    request_body_bytes,
    request_body,
    gun_host = undefined,
    gun_port = undefined,
    gun_path = undefined
}).

init([I, Host, Counter, Client, RequestBodyBytes]) ->
    process_flag(trap_exit, true),
    self() ! request,
    io:format("server ~p started~n", [I]),
    {GunHost, GunPort, GunPath} = gun_destination(Client, Host),
    {ok, #state{
        host = Host,
        counter = Counter,
        client = Client,
        request_body_bytes = RequestBodyBytes,
        request_body = myapp_request_body:get(RequestBodyBytes),
        gun_host = GunHost,
        gun_port = GunPort,
        gun_path = GunPath
    }}.

handle_call(_Name, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(request, State = #state{counter = Counter})->
    {Result, NextState} = request(State),
    case Result of
        ok -> counters:add(Counter, 1, 1);
        _  -> counters:add(Counter, 2, 1)
    end,
    self() ! request,
    {noreply, NextState};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    [].
code_change(_OldVsn, _State, _Extra) ->
    {error, ok}.

request(State = #state{host = Host, client = httpc, request_body = RequestBody}) ->
    Id = base64:encode(crypto:strong_rand_bytes(50)),
    Req = {post,
           Host,
           [{"X-Request-Id", Id}],
           "application/x-www-form-urlencoded",
           iolist_to_binary(RequestBody)},
    Result = myapp_httpc_limiter:request(Req, 30000),
    case Result of
        {ok, {{_, _Status, _}, _, _Response}} ->
            {ok, State};
        Error   ->
            io:format("request error: ~p ~p~n", [Id, Error]),
            {{error, Error}, State}
    end;


request(State = #state{host = Host, client = hackney, request_body = RequestBody}) ->
    Id = base64:encode(crypto:strong_rand_bytes(50)),
    Result = hackney:request(post, Host, [{"X-Request-Id", Id}], RequestBody, [{ssl_options, [{verify, verify_none}]}, {connect_timeout, 5000}]),
    case Result of

        {ok, _StatusCode, _RespHeaders, ClientRef} ->
           
            case hackney:body(ClientRef) of
                {ok, RespBody} when byte_size(RespBody) =:= ?KNOWN_RESPONSE_BODY_BYTES ->
                    {ok, State};
                {ok, RespBody} ->
                    io:format("hackney-error hackney body size mismatch: ~p got=~p expected=~p~n",
                        [Id, byte_size(RespBody), ?KNOWN_RESPONSE_BODY_BYTES]),
                    {{error, {unexpected_body_size, byte_size(RespBody)}}, State};
                 Error   ->
                    io:format("hackney-error body error: ~p ~p~n", [Id, Error]),
                    {{error, Error}, State}
            end;
        Error   ->
            io:format("hackney-error request error: ~p ~p~n", [Id, Error]),
            {{error, Error}, State}
    end;

request(State = #state{client = gun, request_body = RequestBody}) ->
    Id = base64:encode(crypto:strong_rand_bytes(50)),
    Body = iolist_to_binary(RequestBody),
    Headers = [
        {<<"x-request-id">>, Id},
        {<<"user-agent">>, <<"hackney/1.20.1">>},
        {<<"content-type">>, <<"application/octet-stream">>}
    ],
    gun_request(State, Id, Headers, Body).

gun_request(State, Id, Headers, Body) ->
    ok = myapp_gun_limiter:acquire(),
    try
        case open_gun_connection(State) of
            {ok, ConnPid} ->
                try
                    StreamRef = gun:post(ConnPid, State#state.gun_path, Headers, Body),
                    case gun_await_response_body(ConnPid, StreamRef) of
                        ok ->
                            {ok, State};
                        Error ->
                            io:format("gun request error: ~p ~p~n", [Id, Error]),
                            {{error, Error}, State}
                    end
                after
                    catch gun:close(ConnPid)
                end;
            {error, Error} ->
                io:format("gun connection error: ~p ~p~n", [Id, Error]),
                {{error, Error}, State}
        end
    after
        ok = myapp_gun_limiter:release()
    end.

gun_destination(gun, Host) ->
    Parsed = uri_string:parse(Host),
    GunHost = maps:get(host, Parsed),
    GunPort = maps:get(port, Parsed, 443),
    GunPath = case maps:get(path, Parsed, "/") of
        [] -> "/";
        Path -> Path
    end,
    {GunHost, GunPort, GunPath};
gun_destination(_Client, _Host) ->
    {undefined, undefined, undefined}.

open_gun_connection(#state{gun_host = GunHost, gun_port = GunPort}) ->
    Opts = #{
        transport => tls,
        tls_opts => [
            {verify, verify_none},
            {server_name_indication, GunHost}
        ],
        protocols => [http],
        http_opts => #{version => 'HTTP/1.1'},
        retry => 0
    },
    case gun:open(GunHost, GunPort, Opts) of
        {ok, ConnPid} ->
            case gun:await_up(ConnPid, 5000) of
                {ok, _Protocol} ->
                    {ok, ConnPid};
                Error ->
                    catch gun:close(ConnPid),
                    {error, Error}
            end;
        Error ->
            Error
    end.

gun_await_response_body(ConnPid, StreamRef) ->
    case gun:await(ConnPid, StreamRef, 10000) of
        {response, fin, _Status, _Headers} ->
            ok;
        {response, nofin, _Status, Headers} ->
            ExpectedBytes = expected_response_bytes(Headers),
            gun_collect_body(ConnPid, StreamRef, ExpectedBytes, 0);
        Error ->
            Error
    end.

gun_collect_body(ConnPid, StreamRef, ExpectedBytes, ReceivedBytes) ->
    case gun:await(ConnPid, StreamRef, 10000) of
        {data, nofin, Data} ->
            gun_collect_body(ConnPid, StreamRef, ExpectedBytes, ReceivedBytes + byte_size(Data));
        {data, fin, Data} ->
            _FinalBytes = ReceivedBytes + byte_size(Data),
            ok;
        {trailers, _Trailers} ->
            case body_complete(ExpectedBytes, ReceivedBytes) of
                true -> ok;
                false -> {error, {incomplete_body, ReceivedBytes, ExpectedBytes}}
            end;
        {error, {stream_error, Reason}} ->
            case body_complete_on_close(ExpectedBytes, ReceivedBytes) of
                true -> ok;
                false -> {error, {stream_error, Reason, ReceivedBytes, ExpectedBytes}}
            end;
        {error, {connection_error, Reason}} ->
            case body_complete_on_close(ExpectedBytes, ReceivedBytes) of
                true -> ok;
                false -> {error, {connection_error, Reason, ReceivedBytes, ExpectedBytes}}
            end;
        {error, {down, Reason}} ->
            case body_complete_on_close(ExpectedBytes, ReceivedBytes) of
                true -> ok;
                false -> {error, {down, Reason, ReceivedBytes, ExpectedBytes}}
            end;
        {error, timeout} ->
            {error, timeout};
        Error ->
            Error
    end.

expected_response_bytes(Headers) ->
    case lists:keyfind(<<"content-length">>, 1, Headers) of
        {_, Value} ->
            try binary_to_integer(Value) of
                Int -> Int
            catch
                _:_ -> undefined
            end;
        false ->
            undefined
    end.

body_complete(undefined, _ReceivedBytes) ->
    false;
body_complete(ExpectedBytes, ReceivedBytes) ->
    ReceivedBytes >= ExpectedBytes.

body_complete_on_close(undefined, ReceivedBytes) ->
    ReceivedBytes >= ?KNOWN_RESPONSE_BODY_BYTES;
body_complete_on_close(ExpectedBytes, ReceivedBytes) ->
    ReceivedBytes >= ExpectedBytes.
