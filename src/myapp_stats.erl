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
    case maybe_extra_stats(Client) of
        undefined ->
            io:format("[stats ~p size=~p]: Ok:~p, Errors:~p~n", [Client, RequestBodyBytes, Ok, Errors]);
        Extra ->
            io:format("[stats ~p size=~p]: Ok:~p, Errors:~p, ~s~n", [Client, RequestBodyBytes, Ok, Errors, Extra])
    end,
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    [].
code_change(_OldVsn, _State, _Extra) ->
    {error, ok}.

maybe_extra_stats(httpc) ->
    case catch myapp_httpc_limiter:stats() of
        #{profiles := ProfileStats, total_limit := TotalLimit} ->
            FormattedProfiles =
                lists:map(
                  fun(#{profile := Profile, in_flight := InFlight, queue_len := QueueLen, limit := Limit}) ->
                      io_lib:format("~p(InFlight:~p Queue:~p Limit:~p)", [Profile, InFlight, QueueLen, Limit])
                  end,
                  ProfileStats),
            io_lib:format("Profiles:[~s], TotalLimit:~p", [string:join([lists:flatten(Item) || Item <- FormattedProfiles], ", "), TotalLimit]);
        _ ->
            undefined
    end;
maybe_extra_stats(_) ->
    undefined.
