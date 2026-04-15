-module(myapp_error_context).

-export([log/4]).

-define(TABLE, ?MODULE).

log(Client, Id, Error, Meta) ->
    try
        Count = next_count(),
        case should_log(Count) of
            true ->
                io:format(
                    "[error-context] client=~p count=~p id=~p error=~P meta=~P beam=~P os=~P~n",
                    [Client, Count, Id, Error, 20, Meta, 20, beam_snapshot(), 20, os_snapshot(), 20]
                );
            false ->
                ok
        end
    catch
        _:Reason ->
            io:format("[error-context] logging_failed reason=~p client=~p id=~p~n",
                [Reason, Client, Id]),
            ok
    end.

next_count() ->
    ensure_table(),
    ets:update_counter(?TABLE, error_count, {2, 1}, {error_count, 0}).

ensure_table() ->
    case ets:info(?TABLE) of
        undefined ->
            _ = ets:new(?TABLE, [named_table, public, set]),
            ok;
        _ ->
            ok
    end.

should_log(Count) when Count =< 20 ->
    true;
should_log(Count) ->
    Count rem 50 =:= 0.

beam_snapshot() ->
    Mem = erlang:memory(),
    #{
        run_queue => statistics_value(run_queue),
        process_count => erlang:system_info(process_count),
        port_count => erlang:system_info(port_count),
        schedulers_online => erlang:system_info(schedulers_online),
        total_memory => proplists:get_value(total, Mem, undefined),
        processes_memory => proplists:get_value(processes, Mem, undefined),
        binary_memory => proplists:get_value(binary, Mem, undefined),
        ets_memory => proplists:get_value(ets, Mem, undefined),
        atom_memory => proplists:get_value(atom, Mem, undefined),
        system_memory => proplists:get_value(system, Mem, undefined),
        httpc_profiles => httpc_profile_snapshot()
    }.

statistics_value(Key) ->
    case catch erlang:statistics(Key) of
        Value -> Value
    end.

httpc_profile_snapshot() ->
    case catch myapp_httpc_limiter:stats() of
        {'EXIT', _} -> undefined;
        Value -> Value
    end.

os_snapshot() ->
    #{
        loadavg => read_trim("/proc/loadavg"),
        pressure_cpu => read_trim("/proc/pressure/cpu"),
        pressure_memory => read_trim("/proc/pressure/memory"),
        pressure_io => read_trim("/proc/pressure/io"),
        meminfo => read_prefix("/proc/meminfo", 8),
        cgroup_memory_current => read_trim("/sys/fs/cgroup/memory.current"),
        cgroup_memory_events => read_trim("/sys/fs/cgroup/memory.events"),
        cgroup_cpu_stat => read_trim("/sys/fs/cgroup/cpu.stat")
    }.

read_trim(Path) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            string:trim(binary_to_list(Bin));
        _ ->
            unavailable
    end.

read_prefix(Path, Lines) ->
    case file:read_file(Path) of
        {ok, Bin} ->
            string:join(lists:sublist(string:split(binary_to_list(Bin), "\n", all), Lines), "\n");
        _ ->
            unavailable
    end.
