-module(myapp_gun_limiter).
-behaviour(gen_server).

-export([start_link/1, acquire/0, release/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    max_active,
    active = 0,
    waiters = queue:new()
}).

start_link(MaxActive) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [MaxActive], []).

acquire() ->
    gen_server:call(?MODULE, acquire, infinity).

release() ->
    gen_server:call(?MODULE, release, infinity).

init([MaxActive]) ->
    {ok, #state{max_active = MaxActive}}.

handle_call(acquire, _From, State = #state{active = Active, max_active = MaxActive})
  when Active < MaxActive ->
    {reply, ok, State#state{active = Active + 1}};
handle_call(acquire, From, State = #state{waiters = Waiters}) ->
    {noreply, State#state{waiters = queue:in(From, Waiters)}};
handle_call(release, _From, State = #state{active = Active, waiters = Waiters}) ->
    case queue:out(Waiters) of
        {{value, NextFrom}, RestWaiters} ->
            gen_server:reply(NextFrom, ok),
            {reply, ok, State#state{waiters = RestWaiters}};
        {empty, _} when Active > 0 ->
            {reply, ok, State#state{active = Active - 1}};
        {empty, _} ->
            {reply, ok, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
