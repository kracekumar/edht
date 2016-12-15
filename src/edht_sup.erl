%%%-------------------------------------------------------------------
%% @doc edht top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(edht_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([client/5]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).
-define(P, io:format).

%% Node config keys
-define(DATADIR, ".bitcaskdata").
-define(CLIENT_PORT, "client_port").
-define(NODE_PORT, "node_port").
-define(NODE_IPS, "node_ips").
-define(NAME, "name").
-define(REPLICATION, "replication").
-define(NODE_TIMEOUT_MS, "nod_timeout_ms").

-record(client_request, {method, key, value=undefined}).
-record(node_request, {type, client_request}).

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
                          ?REPLICATION => fun list_to_integer/1,
                          ?NODE_TIMEOUT_MS => fun list_to_integer/1,
                          ?NAME => fun(X) -> X end},
    Config = read_config(),
    Map = lists:foldl(fun(Pair, Acc) -> pick(Pair, Acc, ConfigItemTypeMap) end, maps:new(), Config),
    register(config_manager, spawn_link(fun() -> config_manager(Map) end)),

    % Start proc to listen to client
    spawn_link(fun() -> client_listener_loop(get_config(?CLIENT_PORT)) end),

    % Arbiter which handles local io
    register(arbiter, spawn_link(fun() -> arbiter_loop(0) end)),

    % listen to internode communication
    spawn_link(fun() -> listen_to_nodes() end),

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

%% Process which return config values to other process
config_manager(Map) ->
    receive
        {From, Key} ->
            From ! maps:get(Key, Map),
            config_manager(Map)
    end.

%% Config value coordinator
get_config(Key) ->
    %% Function internally calls config_manager to pull the config value
    config_manager ! {self(), Key},
    receive
        Val ->
            Val
    end.

%% Convert IP:Port string into {Host, Port}
ip_string_to_tuple(Str) ->
    Token = string:tokens(Str, ":"),
    Octets = string:tokens(hd(Token), "."),
    IPTuple = list_to_tuple(lists:map(fun(Octet) -> list_to_integer(Octet) end, Octets)),
    {IPTuple, list_to_integer(lists:nth(2, Token))}.

convert_ips_to_tuple(IPString) ->
    %% Convert list of ips into [{{Host}, Port}]
    IPList = string:tokens(IPString, ","),
    lists:map(fun(IP) -> ip_string_to_tuple(IP) end, IPList).

%% Erlang process responsible for key node lookup
node_hash_lookup(NodeRing) ->
    %% Function to keep track of ring and return node[s] responsible for the key
    receive
        {From, lookup, Key} ->
            Node = concha:lookup(Key, NodeRing),
            From ! {lookup, Key, Node, self()},
            node_hash_lookup(NodeRing)
        end.

%% Peristance/lookup from stored file
arbiter_loop(Ref) ->
    receive
        {Caller, Op, Key, Val} ->
            %io:format('Received data for lookup ~p, ~p, ~p~n', [Op, Key, Val]),
            Parent = self(),
            spawn_link(fun() -> handle_io(Parent, Op, Key, Val) end),
            arbiter_loop(Caller);
        {Op, Key, Val} ->
            %io:format('Sending data after lookup ~p, ~p, ~p~n', [Op, Key, Val]),
            Ref ! {arbiter, Op, Key, Val},
            arbiter_loop(Ref);
        Pat -> io:format("failed the pattern, ~p ~n", [Pat])
        end.

handle_node_get(Parent, DBHandle, Key) ->
    BinaryKey = to_binary(Key),
    case bitcask:get(DBHandle, BinaryKey) of
        {ok, Value} ->
            Parent ! {"GET", Key, Value};
        not_found ->
            Parent ! {"GET", Key, undefined}
    end.

handle_node_put(Parent, DBHandle, Key, Val) ->
    BinaryKey = to_binary(Key),
    BinaryVal = to_binary(Val),
    case bitcask:put(DBHandle, BinaryKey, BinaryVal) of
        ok ->
            Parent ! {"PUT", Key, Val};
        {error, _} ->
            Parent ! {"PUT", Key, error}
    end.

handle_io(Parent, Op, Key, Val) ->
    DBHandle = open_store(),
    if 
        Op =:= "GET" ->
            handle_node_get(Parent, DBHandle, Key);
        Op =:= "PUT" ->
            handle_node_put(Parent, DBHandle, Key, Val);
        true -> Parent ! {Op, Key, Val}
    end,
    bitcask:close(DBHandle).

read_config() ->
    application:ensure_all_started(econfig),
    File = os:getenv("CONFIG_FILE"),
    econfig:register_config(config, [File]),
    econfig:get_value(config, "DEFAULT").

open_store() ->
    Dir = ?DATADIR ++ get_config(?NAME),
    bitcask:open(Dir, [read_write]).

listen_to_nodes() ->
    Port = get_config(?NODE_PORT),
    {ok, Socket} = gen_udp:open(Port, [binary, {active, false}]),
    io:format("Listening to node port ~p~n", [Port]),
    node_loop(Socket).

node_loop(Socket) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {udp, Socket, Host, Port, Bin} ->
            {_, Type, {_, Method, Key, Value}} = request_pb:decode_noderequest(Bin),
            NodeRequest = #node_request{type=Type,
                client_request=#client_request{method=Method, key=Key, value=Value}},

            io:format("Received data from node {~p, ~p} on ~p, Method: `~p`, Key: `~p`, Val: `~p`~n",
                      [Host, Port, get_config(?NODE_PORT), Type, Key, Value]),

            spawn_link(fun() -> handle_node_request(Socket, Host, Port, NodeRequest) end),
            node_loop(Socket)
    end.

unpack_node_request(NodeRequest) ->
    {
        NodeRequest#node_request.type,
        {
            NodeRequest#node_request.client_request#client_request.method,
            NodeRequest#node_request.client_request#client_request.key,
            NodeRequest#node_request.client_request#client_request.value
        }
    }.


handle_node_request(Socket, Host, Port, NodeRequest) ->
    {Type, {Method, Key, Value}} = unpack_node_request(NodeRequest),

    %% write to disk (arbiter)
    arbiter ! {self(), Method, Key, Value},

    %% send replicate requests to N - 1 nodes

    receive
        {arbiter, ResponseMethod, ResponseKey, ResponseVal} ->
            io:format('Sending Response to node, Key: ~p, Val: ~p~n', [ResponseKey, ResponseVal]),
            gen_udp:send(Socket, Host, Port, request_pb:encode({clientrequest, ResponseMethod, ResponseKey, ResponseVal}))
    end.

client_listener_loop(Port) ->
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
    inet:setopts(Socket, [binary, {active, once}]),

    receive
        {udp, Socket, Host, Port, RawData} ->
            io:format('----------------~n'),
            io:format("Received data: ~p from `~p` `~p` on `~p`~n", [RawData, Host, Port, get_config(?CLIENT_PORT)]),
            {_, Method, Key, Value} = request_pb:decode_clientrequest(RawData),
            ClientRequest = #client_request{
                method=Method,
                key=Key,
                value=Value
            },

            spawn_link(fun() -> handle_client_request(Socket, Host, Port, ClientRequest) end),
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

unpack_client_request(ClientRequest) ->
    Method = ClientRequest#client_request.method,
    Key = ClientRequest#client_request.key,
    Value = ClientRequest#client_request.value,
    {Method, Key, Value}.

handle_local_connection(Socket, Host, Port, ClientRequest) ->
    {Method, Key, Value} = unpack_client_request(ClientRequest),

    if 
        Method =:= "GET" -> spawn_link(fun() -> handle_client_get(Socket, Host, Port, Key) end);
        Method =:= "PUT" -> spawn_link(fun() -> handle_client_put(Socket, Host, Port, Key, Value) end);
        true -> spawn_link(fun() -> handle_rogue_msg(Socket, Host, Port, Method, Key, Value) end)
    end.

handle_remote_connection(Socket, Host, Port, NodeHost, NodePort, RawData) ->
    spawn_link(fun() -> handle_remote_connection_inner(Socket, Host, Port, NodeHost, NodePort, RawData) end).

handle_remote_connection_inner(Socket, Host, Port, NodeHost, NodePort, ClientRequest) ->
    % If the port number is 0, OS chooses the port for the connection.
    {ok, NodeSocket} = gen_udp:open(0, [binary]),

    {Method, Key, Value} = unpack_client_request(ClientRequest),
    ForwardRequestData = request_pb:encode({noderequest, "FORWARD", {clientrequest, Method, Key, Value}}),
    io:format("noderequest data: ~p~n", [ForwardRequestData]),

    gen_udp:send(NodeSocket, NodeHost, NodePort, ForwardRequestData),
    receive
        {udp, _, _, _, Response} ->
            io:format('Sending data to client: ~p~n', [Response]),
            gen_udp:send(Socket, Host, Port, Response)
    end,
    gen_udp:close(NodeSocket).

handle_client_request(Socket, Host, Port, ClientRequest) ->
    Key = ClientRequest#client_request.key,

    node_hash_lookup ! {self(), lookup, Key},
    {NodeHost, NodePort} = listen_to_lookup_response(),
    CurNodePort = get_config(?NODE_PORT),
    if
        % TODO: Fix IP
        {NodeHost, NodePort} == {{127, 0, 0, 1}, CurNodePort} ->
            handle_local_connection(Socket, Host, Port, ClientRequest);
        true ->
            handle_remote_connection(Socket, Host, Port, NodeHost, NodePort, ClientRequest)
    end.

handle_rogue_msg(Socket, Host, Port, Method, Key, Value) ->
    io:format('rogue'),
    gen_udp:send(Socket, Host, Port, request_pb:encode({clientrequest, Method, Key, Value})).


handle_client_get(Socket, Host, Port, Key) ->
    DBHandle = open_store(),
    case bitcask:get(DBHandle, Key) of
        {ok, Value} ->
            io:format("Key: ~p Value: ~p~n", [Key, Value]),
            gen_udp:send(Socket, Host, Port, request_pb:encode({clientrequest, "GET", Key, Value}));

        not_found ->
            io:format("Key: ~p Value: ~p~n", [Key, not_found]),
            gen_udp:send(Socket, Host, Port, request_pb:encode({clientrequest, "GET", Key, undefined}))
    end,
    bitcask:close(DBHandle).

handle_client_put(Socket, Host, Port, Key, Value) ->
    DBHandle = open_store(),
    case bitcask:put(DBHandle, Key, Value) of
        ok ->
            gen_udp:send(Socket, Host, Port, request_pb:encode({clientrequest, "PUT", Key, Value})),
            io:format("Key: ~p, Value: ~p ~n", [Key, Value]);
        {error, _} ->
            gen_udp:send(Socket, Host, Port, request_pb:encode({clientrequest, "PUT", Key, error})),
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

%% Helper functions to test during development
client(ServerHost, ServerPort, Method, Key, Value) ->
    {ok, Socket} = gen_udp:open(8888, [binary]),
    gen_udp:send(Socket, ServerHost, ServerPort, request_pb:encode(
                                              {clientrequest, Method, Key, Value})),
    receive
        {udp, _, ServerHost, ServerPort, Response} ->
            io:format("Response from Host: ~p, Port: ~p, Response: ~p~n", 
                      [ServerHost, ServerPort, request_pb:decode_clientrequest(Response)])
    after
        5000 ->
            io:format("Timed out listening for response from ~p:~p with request {~p, ~p, ~p}~n",
                [ServerHost, ServerPort, Method, Key, Value])
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
