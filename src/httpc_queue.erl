-module(httpc_queue).
-behaviour(gen_server).

-export([start_link/1, request/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-record(state, {
    limit = 50,
    in_flight = 0,
    queue = queue:new(),
    requests = #{}
}).

start_link(Limit) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [Limit], []).

request(Req, Timeout) ->
    gen_server:call(?MODULE, {request, Req}, Timeout).

init([Limit]) ->
    {ok, #state{limit = Limit}}.

handle_call({request, Req}, From, State = #state{in_flight = N, limit = Limit, requests = Reqs}) when N < Limit ->
    Ref = make_ref(),
    Parent = self(),
    spawn(fun() ->
        Result = do_httpc_request(Req),
        Parent ! {request_done, Ref, Result}
    end),
    {noreply, State#state{
        in_flight = N + 1,
        requests = maps:put(Ref, From, Reqs)
    }};

handle_call({request, Req}, From, State = #state{queue = Q}) ->
    {noreply, State#state{queue = queue:in({From, Req}, Q)}}.

handle_info({request_done, Ref, Result}, State0 = #state{requests = Reqs, in_flight = N}) ->
    From = maps:get(Ref, Reqs),
    gen_server:reply(From, Result),
    State1 = State0#state{
        in_flight = N - 1,
        requests = maps:remove(Ref, Reqs)
    },
    {noreply, maybe_start_next(State1)};

handle_info(_, State) ->
    {noreply, State}.

handle_cast(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.
code_change(_, State, _) ->
    {ok, State}.

maybe_start_next(State = #state{in_flight = N, limit = Limit, queue = Q, requests = Reqs}) when N < Limit ->
    case queue:out(Q) of
        {{value, {From, Req}}, Q1} ->
            Ref = make_ref(),
            Parent = self(),
            spawn(fun() ->
                Result = do_httpc_request(Req),
                Parent ! {request_done, Ref, Result}
            end),
            State#state{
                in_flight = N + 1,
                queue = Q1,
                requests = maps:put(Ref, From, Reqs)
            };
        {empty, _} ->
            State
    end;
maybe_start_next(State) ->
    State.

do_httpc_request({post, Host, Headers, ContentType, Body}) ->
    httpc:request(
        post,
        {Host, Headers, ContentType, Body},
        [{ssl, [{verify, verify_none}]}],
        []
    ).
