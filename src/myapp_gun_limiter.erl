-module(myapp_gun_limiter).
-behaviour(gen_server).

-export([start_link/1, acquire/0, release/0]).
-export([init/1, handle_call/3, handle_cast/2]).

-record(state, {max_active, active = 0, queue = queue:new()}).

start_link(MaxActive) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [MaxActive], []).

acquire() ->
    gen_server:call(?MODULE, acquire, infinity).

release() ->
    gen_server:cast(?MODULE, release).

init([MaxActive]) ->
    {ok, #state{max_active = MaxActive}}.

handle_call(acquire, From, State = #state{active = Active, max_active = MaxActive, queue = Queue}) ->
    case Active < MaxActive of
        true ->
            {reply, ok, State#state{active = Active + 1}};
        false ->
            {noreply, State#state{queue = queue:in(From, Queue)}}
    end.

handle_cast(release, State = #state{active = Active, queue = Queue}) ->
    case queue:out(Queue) of
        {{value, From}, Queue2} ->
            gen_server:reply(From, ok),
            {noreply, State#state{queue = Queue2}};
        {empty, Queue2} ->
            {noreply, State#state{active = erlang:max(Active - 1, 0), queue = Queue2}}
    end.
