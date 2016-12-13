%%%-------------------------------------------------------------------
%% @doc edht top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(edht_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([client/3, client/5, successor/3]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(DATADIR, ".bitcaskdata").
-define(CLIENT_PORT, "client_port").
-define(NODE_PORT, "node_port").
-define(NODE_IPS, "node_ips").
-define(NAME, "name").

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
    % load protobuf file for encoding and decoding
    protobuffs_compile:scan_file("src/request.proto"),
    % read config for listening on different ports
    ConfigItemTypeMap = #{?CLIENT_PORT => fun list_to_integer/1, 
                          ?NODE_PORT => fun list_to_integer/1,
                          ?NODE_IPS => fun convert_ips_to_tuple/1,
                          ?NAME => fun(X) -> X end},
    Config = read_config(),
    Map = lists:foldl(fun(Pair, Acc) -> pick(Pair, Acc, ConfigItemTypeMap) end, maps:new(), Config),
    register(config_manager, spawn_link(fun() -> config_manager(Map) end)),
    % Start proc to listen to client
    spawn_link(fun() -> start_procs() end),
    % Arbiter which handles local io
    Arbiter = spawn_link(fun() -> local_io(0) end),
    % listen to internode communication
    spawn_link(fun() -> listen_to_nodes(Arbiter) end),
    % proc to keep track of nodes
    NodeIPs = get_config(?NODE_IPS),
    io:format("Node IPs: ~p~n", [NodeIPs]),
    NodeRing = concha:new(5, NodeIPs),
    register(node_hash_lookup, spawn_link(fun() -> node_hash_lookup(NodeRing) end)),
    {ok, { {one_for_all, 0, 1}, []} }.

%%====================================================================
%% Internal functions
%%====================================================================

pick(Pair, Acc, ConfigItemTypeMap) ->
    {Key, Val} = Pair,
    Fun = maps:get(Key, ConfigItemTypeMap),
    maps:put(Key, Fun(Val), Acc).

config_manager(Map) ->
    receive
        {From, Key} ->
            From ! maps:get(Key, Map),
            config_manager(Map)
    end.

get_config(Key) ->
    config_manager ! {self(), Key},
    receive
        Val ->
            Val
    end.

ip_string_to_tuple(Str) ->
    Token = string:tokens(Str, ":"),
    Octets = string:tokens(hd(Token), "."),
    IPTuple = list_to_tuple(lists:map(fun(Octet) -> list_to_integer(Octet) end, Octets)),
    {IPTuple, list_to_integer(lists:nth(2, Token))}.

convert_ips_to_tuple(IPString) ->
    %% Convert list of ips into [{{Host}, Port}]
    IPList = string:tokens(IPString, ","),
    lists:map(fun(IP) -> ip_string_to_tuple(IP) end, IPList).

node_hash_lookup(NodeRing) ->
    %% Function to keep track of ring and return node[s] responsible for the key
    receive
        {From, lookup, Key} ->
            Node = concha:lookup(Key, NodeRing),
            From ! {lookup, Key, Node, self()},
            node_hash_lookup(NodeRing)
        end.

local_io(Ref) ->
    receive
        {Caller, RequestCounter, Op, Key, Val} ->
            %io:format('Received data for lookup ~p, ~p, ~p~n', [Op, Key, Val]),
            Parent = self(),
            spawn_link(fun() -> handle_io(Parent, RequestCounter, Op, Key, Val) end),
            local_io(Caller);
        {RequestCounter, Op, Key, Val} ->
            %io:format('Sending data after lookup ~p, ~p, ~p~n', [Op, Key, Val]),
            Ref ! {arbiter, RequestCounter, Op, Key, Val},
            local_io(Ref);
        Pat -> io:format("failed the pattern, ~p ~n", [Pat])
        end.

handle_io(Parent, RequestCounter, Op, Key, Val) ->
    DBHandle = open_store(),
    BinaryKey = to_binary(Key),
    BinaryVal = to_binary(Val),
    if 
        Op =:= "GET" ->
            case bitcask:get(DBHandle, BinaryKey) of
                {ok, Value} ->
                    Parent ! {RequestCounter, Op, Key, Value};
                not_found ->
                    Parent ! {RequestCounter, Op, Key, undefined}
            end;
        Op =:= "PUT" ->
            case bitcask:put(DBHandle, BinaryKey, BinaryVal) of
                ok ->
                    Parent ! {RequestCounter, Op, Key, Val};
                {error, _} ->
                    Parent ! {RequestCounter, Op, Key, error}
            end;
        true -> Parent ! {RequestCounter, Op, Key, Val}
    end,
    bitcask:close(DBHandle).

start_procs() ->
    Port = get_config(?CLIENT_PORT),
    listen_to_clients(Port).

read_config() ->
    application:ensure_all_started(econfig),
    File = os:getenv("CONFIG_FILE"),
    econfig:register_config(config, [File]),
    econfig:get_value(config, "DEFAULT").

open_store() ->
    bitcask:open(?DATADIR, [read_write]).


listen_to_clients(Port) ->
    %% Listen to all UDP clients.
    spawn_link(fun() -> handle_client_connections(Port) end).

listen_to_nodes(Arbiter) ->
    Port = get_config(?NODE_PORT),
    {ok, Socket} = gen_udp:open(Port, [binary, {active, false}]),
    io:format("Listening to node port ~p~n", [Port]),
    node_loop(Arbiter, Socket, maps:new(), 0).


node_loop(Arbiter, Socket, RequestMap, RequestCounter) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {udp, Socket, Host, Port, Bin} ->
            {_, Method, Key, Value} = request_pb:decode_request(Bin),
            io:format("Received data from node {~p, ~p} on ~p, Method: `~p`, Key: `~p`, Val: `~p`~n", 
                      [Host, Port, get_config(?NODE_PORT), Method, Key, Value]),
            NewRequestMap = maps:put(RequestCounter, {Host, Port}, RequestMap),
            Arbiter ! {self(), RequestCounter, Method, Key, Value},
            node_loop(Arbiter, Socket, NewRequestMap, RequestCounter + 1);
        {arbiter, Counter, Method, Key, Val} ->
            io:format('Sending Response to node, Key: ~p, Val: ~p~n', [Key, Val]),
            {Host, Port} = maps:get(Counter, RequestMap),
            NewRequestMap = maps:remove(Counter, RequestMap),
            gen_udp:send(Socket, Host, Port, request_pb:encode({request, Method, Key, Val})),
            node_loop(Arbiter, Socket, NewRequestMap, RequestCounter)
    end.


handle_client_connections(Port) ->
    {ok, Socket} = gen_udp:open(Port, [binary, {active, false}]),
    io:format("Listening to client port ~p~n", [Port]),
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
            spawn_link(fun() -> handle_request(Socket, Host, Port, Method, to_binary(Key), to_binary(Value), Bin) end),
            % Spawn new Proc to send data back?
            client_loop(Socket)
    end.


listen_to_lookup_response() ->
    % Receive message from lookup process and return to caller
    NodeHashLookup = whereis(node_hash_lookup),
    receive 
        {lookup, Key, Node, From} ->
            if 
                From == NodeHashLookup ->
                    io:format("Destination Node for key ~p is ~p~n", [Key, Node]),
                    Node;
                true ->
                    io:format("Not from node lookup")
            end;
        Any -> io:format("Any ~p~n", [Any])
    end.

handle_local_connection(Method, Socket, Host, Port, Key, Value) ->
    if 
        Method =:= "GET" -> spawn_link(fun() -> handle_get(Socket, Host, Port, Key) end);
        Method =:= "PUT" -> spawn_link(fun() -> handle_put(Socket, Host, Port, Key, Value) end);
        true -> spawn_link(fun() -> handle_rogue_msg(Socket, Host, Port, Method, Key, Value) end)
    end.

handle_remote_connection_inner(Socket, Host, Port, NodeHost, NodePort, RawData) ->
    % If the port number is 0, OS chooses the port for the connection.
    {ok, NodeSocket} = gen_udp:open(0, [binary]),
    gen_udp:send(NodeSocket, NodeHost, NodePort, RawData),
    receive
        {udp, _, _, _, Response} ->
            io:format('Sending data to client: ~p~n', [Response]),
            gen_udp:send(Socket, Host, Port, Response)
    end,
    gen_udp:close(NodeSocket).

handle_remote_connection(Socket, Host, Port, NodeHost, NodePort, RawData) ->                                    
    spawn_link(fun() -> handle_remote_connection_inner(Socket, Host, Port, NodeHost, NodePort, RawData) end).

handle_request(Socket, Host, Port, Method, Key, Value, RawData) ->
    node_hash_lookup ! {self(), lookup, Key},
    {NodeHost, NodePort} = listen_to_lookup_response(),
    CurNodePort = get_config(?NODE_PORT),
    if
        % TODO: Fix IP
        {NodeHost, NodePort} == {{127, 0, 0, 1}, CurNodePort} ->
            handle_local_connection(Method, Socket, Host, Port, Key, Value);
        true ->
            handle_remote_connection(Socket, Host, Port, NodeHost, NodePort, RawData)
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
    end,
    bitcask:close(DBHandle).

handle_put(Socket, Host, Port, Key, Value) ->
    DBHandle = open_store(),
    case bitcask:put(DBHandle, Key, Value) of
        ok ->
            gen_udp:send(Socket, Host, Port, request_pb:encode({request, "PUT", Key, Value})),
            io:format("Key: ~p, Value: ~p ~n", [Key, Value]);
        {error, _} ->
            gen_udp:send(Socket, Host, Port, request_pb:encode({request, "PUT", Key, error})),
            io:format("error: Key: ~p, Value: ~p ~n", [Key, Value])
    end,
    bitcask:close(DBHandle).

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

client(Method, Key, Value, Host, Port) ->
    {ok, Socket} = gen_udp:open(8888, [binary]),
    gen_udp:send(Socket, Host, Port, request_pb:encode(
                                              {request, Method, Key, Value})),
    receive
        {udp, _, Host, Port, Response} ->
            io:format("Response from Host: ~p, Port: ~p, Response: ~p~n", 
                      [Host, Port, request_pb:decode_request(Response)])
    end,
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
