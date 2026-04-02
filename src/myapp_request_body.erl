-module(myapp_request_body).

-include("data.hrl").

-export([init/0, get/1, sizes/0]).

-define(KEY, {?MODULE, bodies}).

init() ->
    Bodies =
        maps:from_list(
            [{Size, lists:sublist(?body, Size)} || Size <- sizes()]
        ),
    persistent_term:put(?KEY, Bodies),
    ok.

get(Size) ->
    Bodies = persistent_term:get(?KEY),
    maps:get(Size, Bodies).

sizes() ->
    [1000000, 900000, 800000, 700000, 600000, 500000, 524288, 524289, 400000, 300000, 200000, 100000, 262144, 262145].
