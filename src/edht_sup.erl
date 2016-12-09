%%%-------------------------------------------------------------------
%% @doc edht top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(edht_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([client/3, successor/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    io:format('Starting EDHT~n'),
    protobuffs_compile:scan_file("src/request.proto"),
    %Config = read_config(),
    start_procs(4545),
    {ok, { {one_for_all, 0, 1}, []} }.

%%====================================================================
%% Internal functions
%%====================================================================
-define(DATADIR, ".bitcaskdata").


start_procs(Port) ->
    listen_to_clients(Port).

read_config() ->
    application:ensure_all_started(econfig),
    econfig:register_config(config, ["config.ini"]),
    econfig:get_value(config, "default").

open_store() ->
    bitcask:open(?DATADIR, [read_write]).


listen_to_clients(Port) ->
    %% Listen to all UDP clients.
    spawn_link(fun() -> handle_client_connections(Port) end).

listen_to_nodes(Port) ->
    {ok, Socket} = gen_udp:open(Port, [binary, {active, false}]),
    io:format("Listening to port ~p~n", [Port]),
    node_loop(Socket).


node_loop(Socket) ->
    inet:setopts(Socket, [{active, once}]),

    receive
        {udp, Socket, Host, Port, Bin} ->
            io:format("Received data `~p` `~p` from `~p`~n", [Host, Port, Bin]),
            node_loop(Socket)
    end.


handle_client_connections(Port) ->
    {ok, Socket} = gen_udp:open(Port, [binary, {active, false}]),
    io:format("Listening to port ~p~n", [Port]),
    client_loop(Socket).

to_binary(Val) when is_binary(Val) ->
    Val;
to_binary(Val) when is_atom(Val) ->
    Val;
to_binary(Val) when is_integer(Val) ->
    integer_to_binary(Val);
to_binary(Val) ->
    list_to_binary(Val).

client_loop(Socket) ->
    inet:setopts(Socket, [{active, once}]),

    receive
        {udp, Socket, Host, Port, Bin} ->
            io:format("Received data `~p` `~p` from `~p`~n", [Host, Port, Bin]),
            {_, Method, Key, Value} = request_pb:decode_request(Bin),
            spawn_link(fun() -> handle_request(Socket, Host, Port, Method, to_binary(Key), to_binary(Value)) end),
            % Spawn new Proc to send data back?
            client_loop(Socket)
    end.

handle_request(Socket, Host, Port, Method, Key, Value) ->
    io:format("Key: ~p, Value: ~p, Method: ~p~n",[Key, Value, Method]),
    if 
        Method =:= "GET" -> spawn_link(fun() -> handle_get(Socket, Host, Port, Key) end);
        Method =:= "PUT" -> spawn_link(fun() -> handle_put(Socket, Host, Port, Key, Value) end);
        true -> spawn_link(fun() -> handle_rogue_msg(Socket, Host, Port, Method, Key, Value) end)
    end.
        
handle_rogue_msg(Socket, Host, Port, Method, Key, Value) ->
    io:format('rogue'),
    gen_udp:send(Socket, Host, Port, request_pb:encode({request, Method, Key, Value})).


handle_get(Socket, Host, Port, Key) ->
    DBHandle = open_store(),
    case bitcask:get(DBHandle, Key) of
        {ok, Value} ->
            io:format("Key: ~p Value: ~p~n", [Key, Value]),
            gen_udp:send(Socket, Host, Port, request_pb:encode({request, "GET", Key, Value}));

        not_found ->
            io:format("Key: ~p Value: ~p~n", [Key, not_found]),
            gen_udp:send(Socket, Host, Port, request_pb:encode({request, "GET", Key, undefined}))
    end.

handle_put(Socket, Host, Port, Key, Value) ->
    DBHandle = open_store(),
    case bitcask:put(DBHandle, Key, Value) of
        ok ->
            gen_udp:send(Socket, Host, Port, request_pb:encode({request, "PUT", Key, Value})),
            io:format("Key: ~p, Value: ~p ~n", [Key, Value]);
        {error, _} ->
            gen_udp:send(Socket, Host, Port, request_pb:encode({request, "PUT", Key, error})),
            io:format("error: Key: ~p, Value: ~p ~n", [Key, Value])
    end.

successor(Ring, Key, Length) ->
    Node = concha:lookup(Key, Ring),
    fetch_n_nodes(Node, Ring, Length).

fetch_n_nodes(Node, Ring, Length) ->
    Members = concha:members(Ring),
    StartIndex = index_of(Node, Members),
    DoubleList = lists:append(Members, Members),
    lists:sublist(DoubleList, StartIndex, Length).

index_of(Value, List) ->
    Map = lists:zip(List, lists:seq(1, length(List))),
    case lists:keyfind(Value, 1, Map) of
        {Value, Index} -> Index;
        false -> notfound
    end.


client(Method, Key, Value) ->
    {ok, Socket} = gen_udp:open(8888),
    gen_udp:send(Socket, {127,0,0,1}, 4545, request_pb:encode(
                                              {request, Method, Key, Value})),
    gen_udp:close(Socket).

%%====================================================================
%% Private tests
%%====================================================================
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

index_of_empty_test() ->
    % empty list
    ?assertEqual(notfound, index_of(2, [])).

index_of_notfound_test() ->
    % element not in list
    ?assertEqual(notfound, index_of(10, [1, 2])).

index_of_member_test() ->
    % element is in list
    ?assertEqual(1, index_of(3, [3, 2, 1])).

successor_test() ->
    R = concha:new([1, 2, 3, 4]),
    ?assertEqual(3, length(edht_sup:successor(R, "geoff", 3))).

fetch_n_nodes_test() ->
    R = concha:new([1, 2, 3, 4]),
    ?assertEqual([2, 3, 4], fetch_n_nodes(2, R, 3)).
-endif.
