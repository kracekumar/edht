-module(all_SUITE).
-include_lib("common_test/include/ct.hrl").

-export([all/0]).
-export([test_arbiter_get_missing_value/1,
         test_arbiter_put/1,
         test_arbiter_put_and_get/1,
         test_arbiter_put_and_get_unrelated/1]).

all() ->
    [test_arbiter_get_missing_value,
     test_arbiter_put,
     test_arbiter_put_and_get,
     test_arbiter_put_and_get_unrelated].

get_config_file() ->
    EbinDir = filename:dirname(code:which(?MODULE)),
    AppPath = filename:dirname(EbinDir),
    filename:join([AppPath, "test", "all_SUITE_data", "config.ini"]).

init_per_testcase(_, Config) ->
    %% ignore for all other cases
    setup().

setup() ->
    edht_sup:register_arbiter(),
    io:format("~p~n", [file:get_cwd()]),
    edht_sup:register_config_manager(get_config_file()),

    NodeIPs = edht_sup:get_config("node_ips"),
    NodeRing = concha:new(5, NodeIPs),
    edht_sup:register_node_hash_lookup(NodeRing).

% arbiter tests
call_arbiter_and_assert(In, ToAssert) ->
    Out = call_arbiter(In),
    ToAssert = Out.

call_arbiter(In) ->
    {ReqMethod, ReqKey, ReqValue} = In,
    arbiter ! {self(), ReqMethod, ReqKey, ReqValue},
    receive
        {arbiter, RespMethod, RespKey, RespValue} ->
            {RespMethod, RespKey, RespValue}
    end.

test_arbiter_get_missing_value(_Config) ->
    setup(),
    In = {"GET", "Metamorphosis", undefined},
    call_arbiter_and_assert(In, In).

test_arbiter_put(_Config) ->
    setup(),
    In = {"PUT", "Metamorphosis", "Kafka"},
    call_arbiter_and_assert(In, In).

test_arbiter_put_and_get(_Config) ->
    setup(),
    Key = "Metamorphosis",
    Val = "Kafka",
    InPut = {"PUT", Key, Val},

    call_arbiter(InPut),
    InGet = {"GET", "Metamorphosis", undefined},

    Out = call_arbiter(InGet),
    Out = {"GET", Key, list_to_binary(Val)}.

test_arbiter_put_and_get_unrelated(_Config) ->
    setup(),
    InPut = {"PUT", "Metamorphosis", "Kafka"},
    call_arbiter(InPut),
    InGet = {"GET", "Metamorphosi", undefined},
    call_arbiter_and_assert(InGet, InGet).
