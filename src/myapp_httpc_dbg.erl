-module(myapp_httpc_dbg).
-behaviour(gen_server).

-export([start_link/1, trace_handler/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-record(state, {
    trace_limit
}).

-record(http_response_h,{
      'cache-control',
      connection,
      date,
      pragma,
      trailer,
      'transfer-encoding',
      upgrade,
      via,
      warning,
      'accept-ranges',
      age,
      etag,
      location,
      'proxy-authenticate',
      'retry-after',
      server,
      vary,
      'www-authenticate',
      allow,
      'content-encoding',
      'content-language',
      'content-length' = "-1",
      'content-location',
      'content-md5',
      'content-range',
      'content-type',
      expires,
      'last-modified',
      other=[]
     }).

-record(session,
    {
      id,
      client_close,
      scheme,
      socket,
      socket_type,
      queue_length = 1,
      type,
      available = false
     }).

-record(handler_state,
    {
      request,
      session,
      status_line,
      headers,
      body,
      mfa,
      pipeline,
      keep_alive,
      status,
      canceled = [],
      max_header_size = nolimit,
      max_body_size = nolimit,
      options,
      timers,
      profile_name,
      once = inactive
     }).

-record(request, {
    id,
    from,
    scheme,
    address,
    path,
    pquery,
    method,
    headers,
    content,
    settings,
    abs_uri = false,
    userinfo = false,
    stream = none
}).

start_link(TraceLimit) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [TraceLimit], []).

init([TraceLimit]) ->
    process_flag(trap_exit, true),
    ok = start_dbg_trace(TraceLimit),
    {ok, #state{trace_limit = TraceLimit}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    catch dbg:stop_clear(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

trace_handler(Msg, #{count := Count, limit := Limit} = State) when Count >= Limit ->
    catch dbg:stop_clear(),
    State;
trace_handler(Msg, #{count := Count, limit := Limit} = State) ->
    case maybe_print_trace(Msg) of
        true when Count + 1 >= Limit ->
            io:format("[httpc dbg] trace limit reached, stopping dbg~n", []),
            catch dbg:stop_clear(),
            State#{count => Count + 1};
        true ->
            State#{count => Count + 1};
        false ->
            State
    end.

maybe_print_trace({trace, Pid, call, {httpc_response, error, [Request, socket_closed_remotely]}}) ->
    io:format("[httpc dbg] pid=~p httpc_response:error socket_closed_remotely request=~P~n",
              [Pid, Request, 6]),
    true;
maybe_print_trace({trace, Pid, return_from, {httpc_handler, try_to_enable_pipeline_or_keep_alive, 1},
                   ReturnState}) ->
    Status = handler_status(ReturnState),
    Conn = header_connection(ReturnState),
    Len = header_content_length(ReturnState),
    Available = session_available(ReturnState),
    Type = session_type(ReturnState),
    io:format("[httpc dbg] pid=~p try_to_enable -> status=~p connection=~p content_length=~p session_available=~p session_type=~p~n",
              [Pid, Status, Conn, Len, Available, Type]),
    true;
maybe_print_trace({trace, Pid, call, {httpc_handler, answer_request, [Request, Msg, State]}}) ->
    io:format("[httpc dbg] pid=~p answer_request request_id=~p msg=~p status=~p profile=~p session_available=~p session_type=~p~n",
              [Pid, request_id(Request), short_answer(Msg), handler_status(State), profile_name(State),
               session_available(State), session_type(State)]),
    true;
maybe_print_trace({trace, Pid, call, {httpc_handler, maybe_make_session_available, [Profile,
                   #session{id = Id, available = Available, type = Type, queue_length = QueueLen}]}}) ->
    io:format("[httpc dbg] pid=~p maybe_make_session_available call profile=~p id=~p available=~p type=~p queue=~p~n",
              [Pid, Profile, Id, Available, Type, QueueLen]),
    true;
maybe_print_trace({trace, Pid, return_from, {httpc_handler, maybe_make_session_available, 2},
                   #session{id = Id, available = Available, type = Type, queue_length = QueueLen}}) ->
    io:format("[httpc dbg] pid=~p maybe_make_session_available return id=~p available=~p type=~p queue=~p~n",
              [Pid, Id, Available, Type, QueueLen]),
    true;
maybe_print_trace({trace, Pid, call, {httpc_handler, handle_info, [{ssl_closed, Socket}, _State]}}) ->
    io:format("[httpc dbg] pid=~p httpc_handler:handle_info ssl_closed socket=~p~n",
              [Pid, Socket]),
    true;
maybe_print_trace({trace, Pid, call, {httpc_handler, handle_info, [{tcp_closed, Socket}, _State]}}) ->
    io:format("[httpc dbg] pid=~p httpc_handler:handle_info tcp_closed socket=~p~n",
              [Pid, Socket]),
    true;
maybe_print_trace({trace, Pid, call, {httpc_handler, terminate, [Reason, _State]}}) ->
    io:format("[httpc dbg] pid=~p httpc_handler:terminate reason=~P~n",
              [Pid, Reason, 6]),
    true;
maybe_print_trace(_) ->
    false.

handler_status(#handler_state{status = Status}) ->
    Status;
handler_status(Term) when is_tuple(Term), tuple_size(Term) >= 10, element(1, Term) =:= state ->
    element(10, Term);
handler_status(_) ->
    undefined.

profile_name(#handler_state{profile_name = Profile}) ->
    Profile;
profile_name(Term) when is_tuple(Term), tuple_size(Term) >= 15, element(1, Term) =:= state ->
    element(15, Term);
profile_name(_) ->
    undefined.

request_id(#request{id = Id}) ->
    Id;
request_id(Term) when is_tuple(Term), tuple_size(Term) >= 2, element(1, Term) =:= request ->
    element(2, Term);
request_id(_) ->
    undefined.

short_answer({response, _Code, _Phrase}) ->
    response;
short_answer({failed_connect, _}) ->
    failed_connect;
short_answer({error, Reason}) ->
    {error, Reason};
short_answer(Other) ->
    Other.

session_tuple(#handler_state{session = Session}) ->
    Session;
session_tuple(Term) when is_tuple(Term), tuple_size(Term) >= 3, element(1, Term) =:= state ->
    element(3, Term);
session_tuple(_) ->
    undefined.

session_available(Term) ->
    case session_tuple(Term) of
        #session{available = Available} -> Available;
        Session when is_tuple(Session), tuple_size(Session) >= 9, element(1, Session) =:= session ->
            element(9, Session);
        _ ->
            undefined
    end.

session_type(Term) ->
    case session_tuple(Term) of
        #session{type = Type} -> Type;
        Session when is_tuple(Session), tuple_size(Session) >= 8, element(1, Session) =:= session ->
            element(8, Session);
        _ ->
            undefined
    end.

headers_tuple(#handler_state{headers = Headers}) ->
    Headers;
headers_tuple(Term) when is_tuple(Term), tuple_size(Term) >= 5, element(1, Term) =:= state ->
    element(5, Term);
headers_tuple(_) ->
    undefined.

header_connection(Term) ->
    case headers_tuple(Term) of
        #http_response_h{connection = Conn} -> Conn;
        Headers when is_tuple(Headers), tuple_size(Headers) >= 3, element(1, Headers) =:= http_response_h ->
            element(3, Headers);
        _ ->
            undefined
    end.

header_content_length(Term) ->
    case headers_tuple(Term) of
        #http_response_h{'content-length' = Len} -> Len;
        Headers when is_tuple(Headers), tuple_size(Headers) >= 23, element(1, Headers) =:= http_response_h ->
            element(23, Headers);
        _ ->
            undefined
    end.

start_dbg_trace(TraceLimit) ->
    catch dbg:stop_clear(),
    {ok, _TracerPid} =
        dbg:tracer(process, {fun ?MODULE:trace_handler/2, #{count => 0, limit => TraceLimit}}),
    {ok, _} = dbg:p(all, c),
    {ok, _} = dbg:tpl(httpc_response, error, 2, [{'_', [], []}]),
    {ok, _} = dbg:tpl(httpc_handler, handle_info, 2, [{'_', [], []}]),
    {ok, _} = dbg:tpl(httpc_handler, terminate, 2, [{'_', [], []}]),
    {ok, _} = dbg:tpl(httpc_handler, try_to_enable_pipeline_or_keep_alive, 1, [{'_', [], [{return_trace}]}]),
    {ok, _} = dbg:tpl(httpc_handler, maybe_make_session_available, 2, [{'_', [], [{return_trace}]}]),
    ok.
