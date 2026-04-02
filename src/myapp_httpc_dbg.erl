-module(myapp_httpc_dbg).
-behaviour(gen_server).

-export([start_link/1, trace_handler/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-record(state, {
    trace_limit
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

start_dbg_trace(TraceLimit) ->
    catch dbg:stop_clear(),
    {ok, _TracerPid} =
        dbg:tracer(process, {fun ?MODULE:trace_handler/2, #{count => 0, limit => TraceLimit}}),
    {ok, _} = dbg:p(all, c),
    {ok, _} = dbg:tpl(httpc_response, error, 2, [{'_', [], []}]),
    {ok, _} = dbg:tpl(httpc_handler, handle_info, 2, [{'_', [], []}]),
    {ok, _} = dbg:tpl(httpc_handler, terminate, 2, [{'_', [], []}]),
    ok.
