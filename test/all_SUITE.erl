- module(all_SUITE).
- include_lib("common_test/include/ct.hrl").
- export([all/0]).
- export([test_handle_io_get/1]).

all() ->
    [test_handle_io_get].

get_config_file() ->
    EbinDir = filename:dirname(code:which(?MODULE)),
    AppPath = filename:dirname(EbinDir),
    filename:join([AppPath, "test", "all_SUITE_data", "config.ini"]).

init_per_testcase(_, Config) ->
    %% ignore for all other cases
    edht_sup:register_arbiter(),
    io:format("~p~n", [file:get_cwd()]),
    edht_sup:register_config_manager(get_config_file()),

    NodeIPs = edht_sup:get_config("node_ips"),
    NodeRing = concha:new(5, NodeIPs),
    edht_sup:register_node_hash_lookup(NodeRing).

test_handle_io_get(_Config) ->
    edht_sup:handle_io(self(), "GET", "Metamorphosis", undefined).
    %% receive
    %%     {arbiter, Method, Key, Value} ->
