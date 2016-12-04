%%%-------------------------------------------------------------------
%%% @doc
%%% @end
%%%-------------------------------------------------------------------
-module(edht_sup_test).
-include_lib("eunit/include/eunit.hrl").

%% API
-export([]).

add(A, B) ->
    A + B.

%% A very simple example of a unit test
add_test() ->
    ?assertEqual(25, add(10, 15)).
