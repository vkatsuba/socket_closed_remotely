-module(myapp_server).
-behavior(gen_server).

-define(GUN_CLOSED_RETRIES, 10).
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
    gun_conn_pid = undefined,
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



next_chunk({<<>>, _ChunkSize}) ->
    eof;
next_chunk({Bin, ChunkSize}) when byte_size(Bin) =< ChunkSize ->
    {ok, Bin, {<<>>, ChunkSize}};
next_chunk({Bin, ChunkSize}) ->
    <<Chunk:ChunkSize/binary, Rest/binary>> = Bin,
    {ok, Chunk, {Rest, ChunkSize}}.

request(State = #state{host = Host, client = httpc, request_body = RequestBody}) ->
    Id = base64:encode(crypto:strong_rand_bytes(50)),
    % Result = httpc:request(post, {Host, [{"X-Request-Id", Id}], "application/x-www-form-urlencoded", RequestBody}, [{ssl, [{verify, verify_none}]}], []),
    ChunkSize = 64 * 1024,
    Result = httpc:request(
        post,
        {Host,
         [{"X-Request-Id", Id}],
         "application/x-www-form-urlencoded",
         {chunkify, fun next_chunk/1, {iolist_to_binary(RequestBody), ChunkSize}}},
        [
            {ssl, [{verify, verify_none}]}
        ],
        []
    ),
    case Result of

        {ok, {{_, _Status, _}, _, _Response}} ->
            % io:format("request ok ~p~n", [_Status]),
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
                {ok, _} -> 
                        % io:format("request ok ~p~n", [_Status]),
                        {ok, State};
                 Error   ->
                    io:format("hackney body error: ~p ~p~n", [Id, Error]),
                    {{error, Error}, State}
            end;
        Error   ->
            io:format("request error: ~p ~p~n", [Id, Error]),
            {{error, Error}, State}
    end;

request(State = #state{client = gun, request_body = RequestBody}) ->
    Id = base64:encode(crypto:strong_rand_bytes(50)),
    Body = iolist_to_binary(RequestBody),
    Headers = [
        {<<"x-request-id">>, Id},
        {<<"content-type">>, <<"application/x-www-form-urlencoded">>}
    ],
    ok = myapp_gun_limiter:acquire(),
    try
        gun_request(State, Id, Headers, Body, ?GUN_CLOSED_RETRIES)
    after
        myapp_gun_limiter:release()
    end.

gun_request(State, Id, Headers, Body, RetriesLeft) ->
    case ensure_gun_connection(State) of
        {ok, ConnPid, ConnectedState} ->
            StreamRef = gun:post(ConnPid, ConnectedState#state.gun_path, Headers, Body),
            case gun_await_response_body(ConnPid, StreamRef) of
                ok ->
                    {ok, ConnectedState};
                Error ->
                    gun:close(ConnPid),
                    DisconnectedState = ConnectedState#state{gun_conn_pid = undefined},
                    case RetriesLeft > 0 andalso should_retry_gun_error(Error) of
                        true ->
                            io:format("gun request retry: ~p ~p~n", [Id, Error]),
                            gun_request(DisconnectedState, Id, Headers, Body, RetriesLeft - 1);
                        false ->
                            io:format("gun request error: ~p ~p~n", [Id, Error]),
                            {{error, Error}, DisconnectedState}
                    end
            end;
        {error, Error, DisconnectedState} ->
            io:format("gun connection error: ~p ~p~n", [Id, Error]),
            {{error, Error}, DisconnectedState}
    end.

gun_destination(gun, Host) ->
    Parsed = uri_string:parse(Host),
    GunHost = maps:get(host, Parsed),
    GunPort = maps:get(port, Parsed, 443),
    GunPath =
        case maps:get(path, Parsed, "/") of
            [] -> "/";
            Path -> Path
        end,
    {GunHost, GunPort, GunPath};
gun_destination(_Client, _Host) ->
    {undefined, undefined, undefined}.

ensure_gun_connection(State = #state{gun_conn_pid = ConnPid}) when is_pid(ConnPid) ->
    case is_process_alive(ConnPid) of
        true -> {ok, ConnPid, State};
        false -> ensure_gun_connection(State#state{gun_conn_pid = undefined})
    end;
ensure_gun_connection(State = #state{gun_host = GunHost, gun_port = GunPort}) ->
    Opts = #{
        transport => tls,
        tls_opts => [{verify, verify_none}, {server_name_indication, GunHost}],
        protocols => [http],
        retry => 0
    },
    case gun:open(GunHost, GunPort, Opts) of
        {ok, ConnPid} ->
            case gun:await_up(ConnPid, 5000) of
                {ok, _Protocol} ->
                    {ok, ConnPid, State#state{gun_conn_pid = ConnPid}};
                Error ->
                    gun:close(ConnPid),
                    {error, Error, State#state{gun_conn_pid = undefined}}
            end;
        Error ->
            {error, Error, State#state{gun_conn_pid = undefined}}
    end.

gun_await_response_body(ConnPid, StreamRef) ->
    case gun:await(ConnPid, StreamRef, 10000) of
        {response, fin, _Status, _Headers} ->
            ok;
        {response, nofin, _Status, _Headers} ->
            case gun:await_body(ConnPid, StreamRef, 10000) of
                {ok, _Body} -> ok;
                {ok, _Body, _Trailers} -> ok;
                Error -> Error
            end;
        Error ->
            Error
    end.

should_retry_gun_error({error, Reason}) ->
    should_retry_gun_error(Reason);
should_retry_gun_error({stream_error, closed}) ->
    true;
should_retry_gun_error({stream_error, closing}) ->
    true;
should_retry_gun_error({stream_error, {closed, normal}}) ->
    true;
should_retry_gun_error({down, noproc}) ->
    true;
should_retry_gun_error({down, normal}) ->
    true;
should_retry_gun_error({down, {shutdown, closed}}) ->
    true;
should_retry_gun_error({down, {shutdown, {error, einval}}}) ->
    true;
should_retry_gun_error({shutdown, normal}) ->
    true;
should_retry_gun_error({shutdown, closed}) ->
    true;
should_retry_gun_error(noproc) ->
    true;
should_retry_gun_error(closed) ->
    true;
should_retry_gun_error(closing) ->
    true;
should_retry_gun_error(_) ->
    false.
