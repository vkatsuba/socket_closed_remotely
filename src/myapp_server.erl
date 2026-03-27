-module(myapp_server).
-behavior(gen_server).
-include("data.hrl").
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).

-record(state, {host, client}).

init([I, Host, Client]) ->
    process_flag(trap_exit, true),
    self() ! request,
    io:format("server ~p started~n", [I]),
    {ok, #state{host = Host, client = Client}}.

handle_call(_Name, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(request, State) ->
    ok = myapp_stats:request_started(),
    Result = request(State),
    ok = myapp_stats:request_finished(Result),
    self() ! request,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    [].
code_change(_OldVsn, _State, _Extra) ->
    {error, ok}.



request(#state{host = Host, client = httpc}) ->
    Id = base64:encode(crypto:strong_rand_bytes(50)),
    StartTimeNative = erlang:monotonic_time(),
    Result = httpc:request(post, {Host, [{"X-Request-Id", Id}], "application/x-www-form-urlencoded", ?body},
                           [{ssl, [{verify, verify_none}]}], [{body_format, binary}]),
    case Result of

        {ok, {{_, StatusCode, _}, Headers, ResponseBody}} ->
            #{client => httpc,
              request_identifier => Id,
              elapsed_time_milliseconds => elapsed_time_milliseconds(StartTimeNative),
              status_code => StatusCode,
              response_header_count => length(Headers),
              response_body_bytes => byte_size(ResponseBody),
              outcome => success};
        Error ->
            io:format("request error: ~p ~p~n", [Id, Error]),
            #{client => httpc,
              request_identifier => Id,
              elapsed_time_milliseconds => elapsed_time_milliseconds(StartTimeNative),
              error_type => classify_httpc_error(Error),
              outcome => error,
              raw_error => Error}
    end;


request(#state{host = Host, client = hackney}) ->
    Id = base64:encode(crypto:strong_rand_bytes(50)),
    StartTimeNative = erlang:monotonic_time(),
    Result = hackney:request(post, Host, [{"X-Request-Id", Id}], ?body, [{ssl_options, [{verify, verify_none}]}, {connect_timeout, 5000}]),
    case Result of

        {ok, StatusCode, ResponseHeaders, ClientRef} ->
            case hackney:body(ClientRef) of
                {ok, ResponseBody} ->
                    #{client => hackney,
                      request_identifier => Id,
                      elapsed_time_milliseconds => elapsed_time_milliseconds(StartTimeNative),
                      status_code => StatusCode,
                      response_header_count => length(ResponseHeaders),
                      response_body_bytes => byte_size(ResponseBody),
                      outcome => success};
                Error ->
                    io:format("hackney body error: ~p ~p~n", [Id, Error]),
                    #{client => hackney,
                      request_identifier => Id,
                      elapsed_time_milliseconds => elapsed_time_milliseconds(StartTimeNative),
                      error_type => classify_hackney_error(Error),
                      outcome => error,
                      raw_error => Error}
            end;
        Error ->
            io:format("request error: ~p ~p~n", [Id, Error]),
            #{client => hackney,
              request_identifier => Id,
              elapsed_time_milliseconds => elapsed_time_milliseconds(StartTimeNative),
              error_type => classify_hackney_error(Error),
              outcome => error,
              raw_error => Error}
    end.

elapsed_time_milliseconds(StartTimeNative) ->
    erlang:convert_time_unit(erlang:monotonic_time() - StartTimeNative, native, millisecond).

classify_httpc_error({error, socket_closed_remotely}) ->
    socket_closed_remotely;
classify_httpc_error({error, timeout}) ->
    timeout;
classify_httpc_error({error, {failed_connect, _}}) ->
    failed_connect;
classify_httpc_error({error, Reason}) ->
    Reason;
classify_httpc_error(Reason) ->
    Reason.

classify_hackney_error({error, checkout_timeout}) ->
    checkout_timeout;
classify_hackney_error({error, connect_timeout}) ->
    connect_timeout;
classify_hackney_error({error, closed}) ->
    closed;
classify_hackney_error({error, Reason}) ->
    Reason;
classify_hackney_error(Reason) ->
    Reason.
