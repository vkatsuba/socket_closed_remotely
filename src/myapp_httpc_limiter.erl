-module(myapp_httpc_limiter).
-behaviour(gen_server).

-export([start_link/2, request/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    profile,
    limit = 20,
    in_flight = 0,
    queue = queue:new(),
    requests = #{}
}).

start_link(Profile, Limit) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Profile, Limit], []).

request(Req, Timeout) ->
    gen_server:call(?MODULE, {request, Req}, Timeout).

init([Profile, Limit]) ->
    {ok, #state{profile = Profile, limit = Limit}}.

handle_call({request, Req}, From, State = #state{in_flight = InFlight, limit = Limit})
  when InFlight < Limit ->
    {noreply, start_request(Req, From, State)};
handle_call({request, Req}, From, State = #state{queue = Queue0}) ->
    {noreply, State#state{queue = queue:in({From, Req}, Queue0)}}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info({request_done, Ref, Result}, State0 = #state{requests = Requests, in_flight = InFlight}) ->
    From = maps:get(Ref, Requests),
    gen_server:reply(From, Result),
    State1 = State0#state{
        requests = maps:remove(Ref, Requests),
        in_flight = InFlight - 1
    },
    {noreply, maybe_start_next(State1)};
handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

start_request(Req, From, State0 = #state{profile = Profile, in_flight = InFlight, requests = Requests}) ->
    Ref = make_ref(),
    Parent = self(),
    spawn(fun() ->
        Result = do_httpc_request(Profile, Req),
        Parent ! {request_done, Ref, Result}
    end),
    State0#state{
        in_flight = InFlight + 1,
        requests = maps:put(Ref, From, Requests)
    }.

maybe_start_next(State0 = #state{in_flight = InFlight, limit = Limit, queue = Queue0}) when InFlight < Limit ->
    case queue:out(Queue0) of
        {{value, {From, Req}}, Queue1} ->
            start_request(Req, From, State0#state{queue = Queue1});
        {empty, _} ->
            State0
    end;
maybe_start_next(State) ->
    State.

do_httpc_request(Profile, {post, Url, Headers, ContentType, Body}) ->
    httpc:request(
        post,
        {Url, Headers, ContentType, Body},
        [{ssl, [{verify, verify_none}]}],
        [],
        Profile
    ).
