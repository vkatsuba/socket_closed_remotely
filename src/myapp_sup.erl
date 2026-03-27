-module(myapp_sup).
-behavior(supervisor).
-export([init/1]).
-export([start_link/0]).

start_link() ->
  supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->

    Client = httpc, % httpc | hackney
    CountParallelRequests = 100,
    Host = "https://caddy.localhost",

    ChildSpecs =[#{
      id => list_to_atom("server_" ++ integer_to_list(I)), 
      start => {gen_server, start_link, [{local, list_to_atom("server_" ++ integer_to_list(I))}, myapp_server, [I, Host, Client], []]}
    } || I <- lists:seq(1, CountParallelRequests)],

    {ok, {#{}, [
      #{id => stats, start => {gen_server, start_link, [{local, stats}, myapp_stats, [Client, Host, CountParallelRequests], []]}}
    | ChildSpecs]}}.
