-module(myapp_httpc_dbg).
-behaviour(gen_server).

-export([
    start_link/0,
    trace_handler/2,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).

-record(state, {
    tracer_started = false
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    process_flag(trap_exit, true),
    ok = start_dbg_trace(self()),
    {ok, #state{tracer_started = true}}.

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({httpc_dbg_trace, TraceEvent}, State) ->
    ok = myapp_stats:httpc_trace_event(TraceEvent),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{tracer_started = true}) ->
    dbg:stop_clear(),
    ok;
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

trace_handler(Trace, OwnerPid) ->
    maybe_forward_trace(Trace, OwnerPid),
    OwnerPid.

maybe_forward_trace({trace, _Pid, call, {httpc_handler, handle_info, [Message, _State]}}, OwnerPid) ->
    forward_handler_message(Message, OwnerPid);
maybe_forward_trace({trace_ts, _Pid, call, {httpc_handler, handle_info, [Message, _State]}, _Timestamp}, OwnerPid) ->
    forward_handler_message(Message, OwnerPid);
maybe_forward_trace({trace, _Pid, call, {httpc_handler, terminate, [Reason, _State]}}, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, classify_terminate_reason(Reason)};
maybe_forward_trace({trace_ts, _Pid, call, {httpc_handler, terminate, [Reason, _State]}, _Timestamp}, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, classify_terminate_reason(Reason)};
maybe_forward_trace({trace, _Pid, call, {httpc_manager, handle_call, [{request, _Request}, _From, _State]}}, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, httpc_manager_request};
maybe_forward_trace({trace_ts, _Pid, call, {httpc_manager, handle_call, [{request, _Request}, _From, _State]}, _Timestamp}, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, httpc_manager_request};
maybe_forward_trace({trace, _Pid, call, {httpc_manager, handle_cast, [{request_done, _RequestId}, _State]}}, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, httpc_manager_request_done};
maybe_forward_trace({trace_ts, _Pid, call, {httpc_manager, handle_cast, [{request_done, _RequestId}, _State]}, _Timestamp}, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, httpc_manager_request_done};
maybe_forward_trace(_, _OwnerPid) ->
    ok.

forward_handler_message({tcp_closed, _}, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, httpc_handler_tcp_closed};
forward_handler_message({ssl_closed, _}, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, httpc_handler_ssl_closed};
forward_handler_message({tcp_error, _, _}, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, httpc_handler_tcp_error};
forward_handler_message({ssl_error, _, _}, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, httpc_handler_ssl_error};
forward_handler_message({timeout, _RequestId}, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, httpc_handler_timeout};
forward_handler_message(timeout_queue, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, httpc_handler_timeout_queue};
forward_handler_message({init_error, Reason, _ClientErrMsg}, OwnerPid) ->
    OwnerPid ! {httpc_dbg_trace, classify_init_error(Reason)};
forward_handler_message(_, _OwnerPid) ->
    ok.

classify_terminate_reason(normal) ->
    httpc_handler_terminate_normal;
classify_terminate_reason({shutdown, server_closed}) ->
    httpc_handler_terminate_server_closed;
classify_terminate_reason({tcp_error, _, _}) ->
    httpc_handler_terminate_tcp_error;
classify_terminate_reason({ssl_error, _, _}) ->
    httpc_handler_terminate_ssl_error;
classify_terminate_reason(Reason) ->
    {httpc_handler_terminate_other, Reason}.

classify_init_error(error_connecting) ->
    httpc_handler_init_error_connecting;
classify_init_error(error_sending) ->
    httpc_handler_init_error_sending;
classify_init_error(Reason) ->
    {httpc_handler_init_error_other, Reason}.

start_dbg_trace(OwnerPid) ->
    dbg:stop_clear(),
    {ok, _TracerPid} = dbg:tracer(process, {fun ?MODULE:trace_handler/2, OwnerPid}),
    {ok, _} = dbg:p(all, [call, timestamp]),
    {ok, _} = dbg:tp(httpc_handler, handle_info, 2, [{'_', [], []}]),
    {ok, _} = dbg:tp(httpc_handler, terminate, 2, [{'_', [], []}]),
    {ok, _} = dbg:tp(httpc_manager, handle_call, 3, [{'_', [], []}]),
    {ok, _} = dbg:tp(httpc_manager, handle_cast, 2, [{'_', [], []}]),
    ok.
