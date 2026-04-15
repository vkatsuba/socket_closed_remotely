-module(myapp_httpc_limiter).
-behaviour(gen_server).

-export([start_link/2, request/3, stats/0, profile_for_server/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    profiles = [],
    limit_per_profile = 5,
    inflight = #{},
    queues = #{},
    requests = #{}
}).

start_link(Profiles, LimitPerProfile) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Profiles, LimitPerProfile], []).

request(ServerId, Req, Timeout) ->
    gen_server:call(?MODULE, {request, ServerId, Req}, Timeout).

stats() ->
    gen_server:call(?MODULE, stats).

profile_for_server(ServerId) ->
    gen_server:call(?MODULE, {profile_for_server, ServerId}).

init([Profiles, LimitPerProfile]) ->
    Inflight = maps:from_list([{P, 0} || P <- Profiles]),
    Queues = maps:from_list([{P, queue:new()} || P <- Profiles]),
    {ok, #state{
        profiles = Profiles,
        limit_per_profile = LimitPerProfile,
        inflight = Inflight,
        queues = Queues
    }}.

handle_call({request, ServerId, Req}, From, State0) ->
    Profile = choose_profile(ServerId, State0#state.profiles),
    case maps:get(Profile, State0#state.inflight) < State0#state.limit_per_profile of
        true ->
            {noreply, start_request(Profile, Req, From, State0)};
        false ->
            Queue0 = maps:get(Profile, State0#state.queues),
            Queue1 = queue:in({From, Req}, Queue0),
            Queues1 = maps:put(Profile, Queue1, State0#state.queues),
            {noreply, State0#state{queues = Queues1}}
    end;
handle_call({profile_for_server, ServerId}, _From, State = #state{profiles = Profiles}) ->
    {reply, choose_profile(ServerId, Profiles), State};
handle_call(stats, _From, State = #state{profiles = Profiles, limit_per_profile = Limit, inflight = Inflight, queues = Queues}) ->
    ProfileStats =
        [#{profile => Profile,
           in_flight => maps:get(Profile, Inflight),
           queue_len => queue:len(maps:get(Profile, Queues)),
           limit => Limit}
         || Profile <- Profiles],
    {reply, #{profiles => ProfileStats, total_limit => length(Profiles) * Limit}, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({request_done, Ref, Profile, Result}, State0) ->
    From = maps:get(Ref, State0#state.requests),
    gen_server:reply(From, Result),
    Requests1 = maps:remove(Ref, State0#state.requests),
    Inflight0 = maps:get(Profile, State0#state.inflight),
    Inflight1 = maps:put(Profile, Inflight0 - 1, State0#state.inflight),
    State1 = State0#state{
        requests = Requests1,
        inflight = Inflight1
    },
    {noreply, maybe_start_next(Profile, State1)};
handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

choose_profile(ServerId, Profiles) ->
    N = length(Profiles),
    lists:nth(((ServerId - 1) rem N) + 1, Profiles).

start_request(Profile, Req, From, State0) ->
    Ref = make_ref(),
    Parent = self(),
    spawn(fun() ->
        Result = do_httpc_request(Profile, Req),
        Parent ! {request_done, Ref, Profile, Result}
    end),
    Inflight0 = maps:get(Profile, State0#state.inflight),
    State0#state{
        inflight = maps:put(Profile, Inflight0 + 1, State0#state.inflight),
        requests = maps:put(Ref, From, State0#state.requests)
    }.

maybe_start_next(Profile, State0) ->
    Queue0 = maps:get(Profile, State0#state.queues),
    case queue:out(Queue0) of
        {{value, {From, Req}}, Queue1} ->
            Queues1 = maps:put(Profile, Queue1, State0#state.queues),
            start_request(Profile, Req, From, State0#state{queues = Queues1});
        {empty, _} ->
            State0
    end.

do_httpc_request(Profile, {post, Url, Headers, ContentType, Body}) ->
    httpc:request(
        post,
        {Url, Headers, ContentType, Body},
        [{ssl, [{verify, verify_none}]}],
        [{body_format, binary}],
        Profile
    ).
