% Largest request body profile.
-define(large_request_body, ?body).

% Medium request body profile.
-define(medium_request_body, lists:sublist(?body, 100000)).

% Small request body profile.
-define(small_request_body, lists:sublist(?body, 10000)).
