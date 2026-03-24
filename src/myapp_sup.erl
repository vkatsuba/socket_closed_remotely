-module(myapp_sup).
-behaviour(supervisor).

-export([init/1]).
-export([start_link/0]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    Client = httpc, % httpc | hackney
    CountParallelRequests = 100,
    Host = "https://caddy.localhost",
    Counter = counters:new(2, []),

    Profiles = [profile_1, profile_2, profile_3, profile_4],
    LimitPerProfile = 10,

    maybe_start_profiles(Client, Profiles),

    ExtraChildren =
        case Client of
            httpc ->
                [#{
                    id => httpc_profile_queue,
                    start => {httpc_profile_queue, start_link, [Profiles, LimitPerProfile]}
                }];
            hackney ->
                []
        end,

    ChildSpecs = [#{
        id => list_to_atom("server_" ++ integer_to_list(I)),
        start => {gen_server, start_link,
                  [{local, list_to_atom("server_" ++ integer_to_list(I))},
                   myapp_server,
                   [I, Host, Counter, Client],
                   []]}
    } || I <- lists:seq(1, CountParallelRequests)],

    {ok, {#{},
        ExtraChildren ++
        [
            #{id => stats,
              start => {gen_server, start_link,
                        [{local, stats}, myapp_stats, [Counter, Client], []]}}
            | ChildSpecs
        ]}}.

maybe_start_profiles(httpc, Profiles) ->
    lists:foreach(fun(Profile) ->
        case inets:start(httpc, [{profile, Profile}]) of
            {ok, _Pid} -> ok;
            {error, {already_started, _Pid}} -> ok;
            ok -> ok
        end
    end, Profiles);
maybe_start_profiles(_, _) ->
    ok.
