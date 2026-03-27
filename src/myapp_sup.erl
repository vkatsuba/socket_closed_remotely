-module(myapp_sup).
-behavior(supervisor).
-export([init/1]).
-export([start_link/0]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->

    Client = httpc, % httpc | hackney
    CountParallelRequests = 100,
    RequestBodyProfile = large, % small | medium | large
    EnableHttpcDebugTrace = true,
    Host = "https://caddy.localhost",

    OptionalChildSpecs =
        case Client =:= httpc andalso EnableHttpcDebugTrace of
            true ->
                [#{id => myapp_httpc_dbg, start => {myapp_httpc_dbg, start_link, []}}];
            false ->
                []
        end,

    ChildSpecs =[#{
      id => list_to_atom("server_" ++ integer_to_list(I)), 
      start => {gen_server, start_link, [{local, list_to_atom("server_" ++ integer_to_list(I))}, myapp_server, [I, Host, Client, RequestBodyProfile], []]}
    } || I <- lists:seq(1, CountParallelRequests)],

    {ok, {#{}, [
      #{id => stats, start => {gen_server, start_link, [{local, stats}, myapp_stats, [Client, Host, CountParallelRequests, RequestBodyProfile], []]}}
    | OptionalChildSpecs ++ ChildSpecs]}}.
