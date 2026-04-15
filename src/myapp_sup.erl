-module(myapp_sup).
-behavior(supervisor).
-export([init/1]).
-export([start_link/0]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->

    Client = httpc, % httpc | hackney | gun
    CountParallelRequests = 100,
    GunMaxActiveRequests = 50,
    HttpcProfiles = [profile_1, profile_2, profile_3, profile_4, profile_5],
    HttpcLimitPerProfile = 50,
    RequestBodyBytes = 1000000,
    EnableHttpcDebugTrace = false,
    TraceLimit = 1000,
    Host = "https://caddy.localhost",
    Counter = counters:new(2, []),
    ok = myapp_request_body:init(),
    ok = maybe_start_httpc_profiles(Client, HttpcProfiles),

    OptionalChildSpecs =
        (case Client =:= gun of
            true ->
                [#{id => gun_limiter, start => {myapp_gun_limiter, start_link, [GunMaxActiveRequests]}}];
            false ->
                []
         end) ++
        (case Client =:= httpc of
            true ->
                [#{id => httpc_limiter, start => {myapp_httpc_limiter, start_link, [HttpcProfiles, HttpcLimitPerProfile]}}];
            false ->
                []
         end) ++
        (case Client =:= httpc andalso EnableHttpcDebugTrace of
            true ->
                [#{id => httpc_dbg, start => {myapp_httpc_dbg, start_link, [TraceLimit]}}];
            false ->
                []
         end),
    ChildSpecs =[#{
      id => list_to_atom("server_" ++ integer_to_list(I)), 
      start => {gen_server, start_link, [{local, list_to_atom("server_" ++ integer_to_list(I))}, myapp_server, [I, Host, Counter, Client, RequestBodyBytes], []]}
    } || I <- lists:seq(1, CountParallelRequests)],

    {ok, {#{}, [
      #{id => stats, start => {gen_server, start_link, [{local, stats}, myapp_stats, [Counter, Client, RequestBodyBytes], []]}}
    | OptionalChildSpecs ++ ChildSpecs]}}.

maybe_start_httpc_profiles(httpc, Profiles) ->
    lists:foreach(
      fun(Profile) ->
          case inets:start(httpc, [{profile, Profile}]) of
              {ok, _Pid} -> ok;
              {error, {already_started, _Pid}} -> ok;
              ok -> ok
          end
      end,
      Profiles),
    ok;
maybe_start_httpc_profiles(_, _) ->
    ok.
