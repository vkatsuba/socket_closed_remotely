-module(myapp_stats).
-behavior(gen_server).
-export([    
    request_started/0,
    request_finished/1,
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3,
    terminate/2
]).

-record(state, {
    client,
    host,
    configured_parallel_requests,
    total_started = 0, % Requests that entered execution.
    total_succeeded = 0, % Requests that completed successfully.
    total_failed = 0, % Requests that completed with an error.
    current_in_flight_requests = 0, % Requests running right now.
    peak_in_flight_requests = 0, % Highest observed in-flight load.
    total_elapsed_time_milliseconds = 0, % Sum of request durations.
    maximum_elapsed_time_milliseconds = 0, % Slowest observed request.
    total_response_body_bytes = 0, % Sum of successful response sizes.
    maximum_response_body_bytes = 0, % Largest observed response size.
    error_type_counts = #{}, % Errors grouped by exact type.
    previous_total_started = 0, % Previous total started snapshot.
    previous_total_succeeded = 0, % Previous total succeeded snapshot.
    previous_total_failed = 0, % Previous total failed snapshot.
    previous_total_elapsed_time_milliseconds = 0, % Previous duration sum snapshot.
    previous_total_response_body_bytes = 0
}).

request_started() ->
    gen_server:cast(stats, request_started).

request_finished(Result) ->
    gen_server:cast(stats, {request_finished, Result}).

init([Client, Host, CountParallelRequests]) ->
    process_flag(trap_exit, true),
    timer:send_interval(1000, timer),
    {ok, #state{
        client = Client,
        host = Host,
        configured_parallel_requests = CountParallelRequests
    }}.

handle_call(_Name, _From, State) ->
    {reply, ok, State}.

handle_cast(request_started, State = #state{
    total_started = TotalStarted,
    current_in_flight_requests = CurrentInFlightRequests,
    peak_in_flight_requests = PeakInFlightRequests
}) ->
    NewCurrentInFlightRequests = CurrentInFlightRequests + 1,
    {noreply, State#state{
        total_started = TotalStarted + 1,
        current_in_flight_requests = NewCurrentInFlightRequests,
        peak_in_flight_requests = erlang:max(PeakInFlightRequests, NewCurrentInFlightRequests)
    }};
handle_cast({request_finished, Result}, State) ->
    {noreply, apply_request_result(Result, State)};
handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(timer, State0) ->
    Snapshot = snapshot_metrics(State0),
    print_snapshot(Snapshot),
    State = store_previous_totals(State0),
    {noreply, State};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    [].
code_change(_OldVsn, _State, _Extra) ->
    {error, ok}.

apply_request_result(Result, State0 = #state{
    total_succeeded = TotalSucceeded,
    total_failed = TotalFailed,
    current_in_flight_requests = CurrentInFlightRequests,
    total_elapsed_time_milliseconds = TotalElapsedTimeMilliseconds,
    maximum_elapsed_time_milliseconds = MaximumElapsedTimeMilliseconds,
    total_response_body_bytes = TotalResponseBodyBytes,
    maximum_response_body_bytes = MaximumResponseBodyBytes,
    error_type_counts = ErrorTypeCounts
}) ->
    ElapsedTimeMilliseconds = maps:get(elapsed_time_milliseconds, Result, 0),
    ResponseBodyBytes = maps:get(response_body_bytes, Result, 0),
    Outcome = maps:get(outcome, Result),
    State1 = State0#state{
        current_in_flight_requests = erlang:max(CurrentInFlightRequests - 1, 0),
        total_elapsed_time_milliseconds = TotalElapsedTimeMilliseconds + ElapsedTimeMilliseconds,
        maximum_elapsed_time_milliseconds = erlang:max(MaximumElapsedTimeMilliseconds, ElapsedTimeMilliseconds),
        total_response_body_bytes = TotalResponseBodyBytes + ResponseBodyBytes,
        maximum_response_body_bytes = erlang:max(MaximumResponseBodyBytes, ResponseBodyBytes)
    },
    case Outcome of
        success ->
            State1#state{total_succeeded = TotalSucceeded + 1};
        error ->
            ErrorType = maps:get(error_type, Result, unknown_error),
            State1#state{
                total_failed = TotalFailed + 1,
                error_type_counts = increment_map_counter(ErrorType, ErrorTypeCounts)
            }
    end.

increment_map_counter(Key, Counts) ->
    maps:update_with(Key, fun(Value) -> Value + 1 end, 1, Counts).

snapshot_metrics(#state{
    client = Client,
    host = Host,
    configured_parallel_requests = ConfiguredParallelRequests,
    total_started = TotalStarted,
    total_succeeded = TotalSucceeded,
    total_failed = TotalFailed,
    current_in_flight_requests = CurrentInFlightRequests,
    peak_in_flight_requests = PeakInFlightRequests,
    total_elapsed_time_milliseconds = TotalElapsedTimeMilliseconds,
    maximum_elapsed_time_milliseconds = MaximumElapsedTimeMilliseconds,
    total_response_body_bytes = TotalResponseBodyBytes,
    maximum_response_body_bytes = MaximumResponseBodyBytes,
    error_type_counts = ErrorTypeCounts,
    previous_total_started = PreviousTotalStarted,
    previous_total_succeeded = PreviousTotalSucceeded,
    previous_total_failed = PreviousTotalFailed,
    previous_total_elapsed_time_milliseconds = PreviousTotalElapsedTimeMilliseconds,
    previous_total_response_body_bytes = PreviousTotalResponseBodyBytes
}) ->
    TotalCompletedRequests = TotalSucceeded + TotalFailed,
    IntervalStartedRequests = TotalStarted - PreviousTotalStarted,
    IntervalSucceededRequests = TotalSucceeded - PreviousTotalSucceeded,
    IntervalFailedRequests = TotalFailed - PreviousTotalFailed,
    IntervalElapsedTimeMilliseconds = TotalElapsedTimeMilliseconds - PreviousTotalElapsedTimeMilliseconds,
    IntervalResponseBodyBytes = TotalResponseBodyBytes - PreviousTotalResponseBodyBytes,
    #{
        client => Client,
        host => Host,
        configured_parallel_requests => ConfiguredParallelRequests,
        total_started => TotalStarted,
        total_succeeded => TotalSucceeded,
        total_failed => TotalFailed,
        total_completed_requests => TotalCompletedRequests,
        current_in_flight_requests => CurrentInFlightRequests,
        peak_in_flight_requests => PeakInFlightRequests,
        requests_started_per_second => IntervalStartedRequests,
        requests_succeeded_per_second => IntervalSucceededRequests,
        requests_failed_per_second => IntervalFailedRequests,
        average_elapsed_time_milliseconds =>
            safe_division(TotalElapsedTimeMilliseconds, TotalCompletedRequests),
        interval_average_elapsed_time_milliseconds =>
            safe_division(IntervalElapsedTimeMilliseconds, IntervalSucceededRequests + IntervalFailedRequests),
        maximum_elapsed_time_milliseconds => MaximumElapsedTimeMilliseconds,
        average_response_body_bytes =>
            safe_division(TotalResponseBodyBytes, TotalSucceeded),
        interval_average_response_body_bytes =>
            safe_division(IntervalResponseBodyBytes, IntervalSucceededRequests),
        maximum_response_body_bytes => MaximumResponseBodyBytes,
        error_type_counts => ErrorTypeCounts,
        open_file_descriptor_count => read_open_file_descriptor_count(),
        open_file_descriptor_limit => read_open_file_descriptor_limit(),
        erlang_port_count => erlang:system_info(port_count),
        erlang_process_count => erlang:system_info(process_count),
        erlang_run_queue_length => erlang:statistics(run_queue),
        erlang_scheduler_count => erlang:system_info(schedulers_online),
        erlang_memory_total_bytes => erlang:memory(total),
        linux_socket_summary => read_linux_socket_summary(),
        linux_ephemeral_port_range => read_linux_ephemeral_port_range(),
        linux_tcp_fin_timeout => read_linux_sysctl("/proc/sys/net/ipv4/tcp_fin_timeout"),
        linux_tcp_tw_reuse => read_linux_sysctl("/proc/sys/net/ipv4/tcp_tw_reuse"),
        linux_somaxconn => read_linux_sysctl("/proc/sys/net/core/somaxconn"),
        linux_kernel_release => read_linux_kernel_release(),
        linux_operating_system_name => read_linux_operating_system_name()
    }.

store_previous_totals(State = #state{
    total_started = TotalStarted,
    total_succeeded = TotalSucceeded,
    total_failed = TotalFailed,
    total_elapsed_time_milliseconds = TotalElapsedTimeMilliseconds,
    total_response_body_bytes = TotalResponseBodyBytes
}) ->
    State#state{
        previous_total_started = TotalStarted,
        previous_total_succeeded = TotalSucceeded,
        previous_total_failed = TotalFailed,
        previous_total_elapsed_time_milliseconds = TotalElapsedTimeMilliseconds,
        previous_total_response_body_bytes = TotalResponseBodyBytes
    }.

print_snapshot(Snapshot) ->
    io:format(
        "[metrics request] client=~p host=~s configured_parallel_requests=~p total_started=~p total_succeeded=~p total_failed=~p total_completed_requests=~p current_in_flight_requests=~p peak_in_flight_requests=~p requests_started_per_second=~p requests_succeeded_per_second=~p requests_failed_per_second=~p average_elapsed_time_milliseconds=~p interval_average_elapsed_time_milliseconds=~p maximum_elapsed_time_milliseconds=~p average_response_body_bytes=~p interval_average_response_body_bytes=~p maximum_response_body_bytes=~p~n"
        "[metrics errors] client=~p host=~s error_type_counts=~s~n"
        "[metrics system] client=~p host=~s open_file_descriptor_count=~p open_file_descriptor_limit=~p erlang_port_count=~p erlang_process_count=~p erlang_run_queue_length=~p erlang_scheduler_count=~p erlang_memory_total_bytes=~p linux_socket_summary=~s linux_ephemeral_port_range=~s linux_tcp_fin_timeout=~s linux_tcp_tw_reuse=~s linux_somaxconn=~s linux_kernel_release=~s linux_operating_system_name=~s~n",
        [maps:get(client, Snapshot),
         maps:get(host, Snapshot),
         maps:get(configured_parallel_requests, Snapshot),
         maps:get(total_started, Snapshot),
         maps:get(total_succeeded, Snapshot),
         maps:get(total_failed, Snapshot),
         maps:get(total_completed_requests, Snapshot),
         maps:get(current_in_flight_requests, Snapshot),
         maps:get(peak_in_flight_requests, Snapshot),
         maps:get(requests_started_per_second, Snapshot),
         maps:get(requests_succeeded_per_second, Snapshot),
         maps:get(requests_failed_per_second, Snapshot),
         maps:get(average_elapsed_time_milliseconds, Snapshot),
         maps:get(interval_average_elapsed_time_milliseconds, Snapshot),
         maps:get(maximum_elapsed_time_milliseconds, Snapshot),
         maps:get(average_response_body_bytes, Snapshot),
         maps:get(interval_average_response_body_bytes, Snapshot),
         maps:get(maximum_response_body_bytes, Snapshot),
         maps:get(client, Snapshot),
         maps:get(host, Snapshot),
         format_error_type_counts(maps:get(error_type_counts, Snapshot)),
         maps:get(client, Snapshot),
         maps:get(host, Snapshot),
         maps:get(open_file_descriptor_count, Snapshot),
         maps:get(open_file_descriptor_limit, Snapshot),
         maps:get(erlang_port_count, Snapshot),
         maps:get(erlang_process_count, Snapshot),
         maps:get(erlang_run_queue_length, Snapshot),
         maps:get(erlang_scheduler_count, Snapshot),
         maps:get(erlang_memory_total_bytes, Snapshot),
         maps:get(linux_socket_summary, Snapshot),
         maps:get(linux_ephemeral_port_range, Snapshot),
         maps:get(linux_tcp_fin_timeout, Snapshot),
         maps:get(linux_tcp_tw_reuse, Snapshot),
         maps:get(linux_somaxconn, Snapshot),
         maps:get(linux_kernel_release, Snapshot),
         maps:get(linux_operating_system_name, Snapshot)]).

format_error_type_counts(ErrorTypeCounts) when map_size(ErrorTypeCounts) =:= 0 ->
    "none";
format_error_type_counts(ErrorTypeCounts) ->
    Pairs = maps:to_list(ErrorTypeCounts),
    Strings = [io_lib:format("~p=~p", [Key, Value]) || {Key, Value} <- Pairs],
    lists:flatten(string:join([lists:flatten(String) || String <- Strings], ",")).

safe_division(_, 0) ->
    0;
safe_division(Value, Divisor) ->
    Value div Divisor.

read_open_file_descriptor_count() ->
    case file:list_dir("/proc/self/fd") of
        {ok, FileDescriptors} -> length(FileDescriptors);
        _ -> unavailable
    end.

read_open_file_descriptor_limit() ->
    case file:read_file("/proc/self/limits") of
        {ok, Contents} ->
            parse_open_file_descriptor_limit(binary_to_list(Contents));
        _ ->
            unavailable
    end.

parse_open_file_descriptor_limit(Contents) ->
    Lines = string:split(Contents, "\n", all),
    parse_open_file_descriptor_limit_lines(Lines).

parse_open_file_descriptor_limit_lines([]) ->
    unavailable;
parse_open_file_descriptor_limit_lines([Line | Rest]) ->
    case string:find(Line, "Max open files") of
        nomatch ->
            parse_open_file_descriptor_limit_lines(Rest);
        _ ->
            Tokens = string:tokens(Line, " "),
            case Tokens of
                ["Max", "open", "files", SoftLimit | _] -> SoftLimit;
                _ -> Line
            end
    end.

read_linux_socket_summary() ->
    case file:read_file("/proc/net/sockstat") of
        {ok, Contents} ->
            compact_linux_value(binary_to_list(Contents));
        _ ->
            unavailable
    end.

read_linux_ephemeral_port_range() ->
    read_linux_sysctl("/proc/sys/net/ipv4/ip_local_port_range").

read_linux_sysctl(Path) ->
    case file:read_file(Path) of
        {ok, Contents} ->
            compact_linux_value(binary_to_list(Contents));
        _ ->
            unavailable
    end.

read_linux_kernel_release() ->
    case file:read_file("/proc/sys/kernel/osrelease") of
        {ok, Contents} ->
            compact_linux_value(binary_to_list(Contents));
        _ ->
            unavailable
    end.

read_linux_operating_system_name() ->
    case file:read_file("/etc/os-release") of
        {ok, Contents} ->
            parse_operating_system_name(binary_to_list(Contents));
        _ ->
            unavailable
    end.

parse_operating_system_name(Contents) ->
    Lines = string:split(Contents, "\n", all),
    parse_operating_system_name_lines(Lines).

parse_operating_system_name_lines([]) ->
    unavailable;
parse_operating_system_name_lines([Line | Rest]) ->
    case string:prefix(Line, "PRETTY_NAME=") of
        nomatch ->
            parse_operating_system_name_lines(Rest);
        Value ->
            string:trim(Value, both, "\"")
    end.

compact_linux_value(Value) ->
    string:join(string:tokens(string:trim(Value), "\n"), "; ").
