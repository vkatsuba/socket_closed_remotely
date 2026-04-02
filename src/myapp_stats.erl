-module(myapp_stats).
-behavior(gen_server).
-export([    
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).

-record(state, {counter, client, request_body_bytes}).

init([Counter, Client, RequestBodyBytes]) ->
    process_flag(trap_exit, true),
    timer:send_interval(1000, timer),
    {ok, #state{counter = Counter, client = Client, request_body_bytes = RequestBodyBytes}}.

handle_call(_Name, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(timer, State = #state{client = Client, counter = Counter, request_body_bytes = RequestBodyBytes})->
    Ok = counters:get(Counter, 1),
    Errors = counters:get(Counter, 2),
    io:format("[stats ~p size=~p]: Ok:~p, Errors:~p~n", [Client, RequestBodyBytes, Ok, Errors]),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    [].
code_change(_OldVsn, _State, _Extra) ->
    {error, ok}.
