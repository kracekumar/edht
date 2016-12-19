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
-define(NODE_TIMEOUT_MS, "node_timeout_ms").

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
init(Args) ->
    io:format('Starting EDHT with: ~p~n', [Args]),
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
            Val = maps:get(Key, Map),
            From ! {config_manager, Val},
            config_manager(Map)
    end.

%% Config value coordinator
get_config(Key) ->
    %% Function internally calls config_manager to pull the config value
    config_manager ! {self(), Key},
    receive
        {config_manager, Val} ->
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
            node_hash_lookup(NodeRing);
        {From, successor, Key, Length} ->
            Nodes = successor(NodeRing, Key, Length),
            From ! {successor, Nodes},
            node_hash_lookup(NodeRing)
        end.

%% Peristance/lookup from stored file
arbiter_loop(Ref) ->
    receive
        {Caller, Op, Key, Val} ->
            io:format('Received data for lookup ~p, ~p, ~p~n', [Op, Key, Val]),
            Parent = self(),
            spawn_link(fun() -> handle_io(Parent, Op, Key, Val) end),
            arbiter_loop(Caller);
        {Op, Key, Val} ->
            io:format('Sending data after lookup ~p, ~p, ~p~n', [Op, Key, Val]),
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
        {error, Reason} ->
            ?P("Put failed for key: ~p for ~p~n", [BinaryKey, Reason]),
            Parent ! {"PUT", Key, error}
    end.

handle_io(Parent, Op, Key, Val) ->
    case Op of
        "GET" ->
            DBHandle = open_store(),
            handle_node_get(Parent, DBHandle, Key),
            bitcask:close(DBHandle);
        "PUT" ->
            DBHandle = open_store(),
            % bitcask writes data to disk. Every key is mapped to one file.
            % So updating same key across multiple process requires acquiring the lock.
            % Inorder to handle atleast 100 requests or so for same key sleep is used.
            timer:sleep(10),
            handle_node_put(Parent, DBHandle, Key, Val),
            bitcask:close(DBHandle);
        true -> Parent ! {Op, Key, Val}
    end.

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

spin_node_request_process(Data) ->
    {Type, ClientRequest, Socket, Host, Port} = Data,
    {Method, Key, Val} = ClientRequest,
    NodeRequest = #node_request{type=Type, 
                                client_request=#client_request{method=Method, key=Key, value=Val}
                               },
    spawn_link(fun() -> handle_node_request(Socket, Host, Port, NodeRequest) end).

node_loop(Socket) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {udp, Socket, Host, Port, Bin} ->
            {Type, Status, ClientRequests} = decode_node_request(Bin),

            io:format("Received data from node {~p, ~p} on ~p, Type: `~p`, Status: `~p`, Data: `~p`~n",
                      [Host, Port, get_config(?NODE_PORT), Type, Status, ClientRequests]),

            lists:map(fun(X) -> spin_node_request_process({Type, X, Socket, Host, Port}) end, 
                      ClientRequests),
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

pack_client_request(ClientRequest) ->
    Size = tuple_size(ClientRequest),
    case Size of
        4 ->
            % Response from aribiter
            {_, Key, Method, Val} = ClientRequest;
        3 ->
            % Response from peers
            {_, _, Rest} = ClientRequest,
            {Key, Method, Val} = Rest
    end,
    {clientrequest, Method, Key, Val}.

unpack_client_request(ClientRequest) ->
    {_, Method, Key, Val} = ClientRequest,
    {Method, Key, Val}.

encode_node_request(Type, Status, Responses) ->
    ClientRequests = lists:filter(fun(X)-> X =/= {"error"} end, Responses),
    % ?P("Client Requests: ~p~n", [ClientRequests]),
    Resps = lists:map(fun(X) -> pack_client_request(X) end, ClientRequests),
    request_pb:encode({noderequest, Type, Status, Resps}).

decode_node_request(Data) ->
    {_, Type, Status, NodeRequests} = request_pb:decode_noderequest(Data),
    ClientRequests = lists:map(fun(X) -> unpack_client_request(X) end, NodeRequests),
    {Type, Status, ClientRequests}.

handle_node_request(Socket, Host, Port, NodeRequest) ->
    ?P("NodeRequest: ~p~n", [NodeRequest]),
    {Type, {Method, Key, Value}} = unpack_node_request(NodeRequest),
    ?P("Handle node request type ~p~n", [Type]),
    case Type of
         "FORWARD_REQ" ->
            % this node is the coordinator for replication; send replica requests to other
            arbiter ! {self(), Method, Key, Value},
            handle_forward_request(Socket, Host, Port, NodeRequest);
        "FORWARD_RESP" ->
            % This node is not in the replica list: send info back to client
            handle_forward_response(Socket, Host, Port, NodeRequest);
        "REPLICA_REQ" ->
            % this node is a replica
            arbiter ! {self(), Method, Key, Value},
            handle_replica_request(Socket, Host, Port, NodeRequest);
        true -> ?P("Failed~n")
    end.

response_type(RequestType) ->
    case RequestType of
        "FORWARD_REQ" -> "FORWARD_RESP";
        "REPLICA_REQ" -> "REPLICA_RESP";
        "FORWARD_RESP" -> "FORWARD_RESP"
    end.

serialize_node_request(NodeRequest) ->
    {RequestType, {Method, Key, Val}} = unpack_node_request(NodeRequest),
    request_pb:encode({noderequest, response_type(RequestType), undefined, [{clientrequest, Method, Key, Val}]}).

deserialize_node_response(Bin) ->
    ?P("Deserializing node response: ~p~n", [Bin]),
    {_, Type, Status, ClientRequests} = request_pb:decode_noderequest(Bin),
    {_, Method, Key, Val} = hd(ClientRequests),
    {ok, Type, {Method, Key, Val}}.

send_replica_request(From, ReplicaNode, SerializedNodeRequest) ->
    {Host, Port} = ReplicaNode,
    {ok, Socket} = gen_udp:open(0, [binary]),
    ?P("Sending replica request ~p:~p~n", [Host, Port]),
    gen_udp:send(Socket, Host, Port, SerializedNodeRequest),
    TimeOut = get_config(?NODE_TIMEOUT_MS),
    receive
        {udp, _, _, _, Bin} ->
            From ! deserialize_node_response(Bin)
    after
        TimeOut ->
            ?P("Timeout for request to ~p:~p~n", [Host, Port]),
            {"error"}
    end.

coordinate_replicas_recurse(Counter, Acc) ->
    receive
        Data ->
            if 
                Counter == 0 -> [Data | Acc];
                true -> coordinate_replicas_recurse(Counter - 1, [Data | Acc])
            end
    end.

coordinate_replicas(ReplicaNodes, NodeRequest) ->
    SerializedNodeRequest = serialize_node_request(NodeRequest),
    From = self(),
    Counter = length(ReplicaNodes),
    lists:map(fun(ReplicaNode) -> spawn_link(fun() -> send_replica_request(From, ReplicaNode, SerializedNodeRequest) end)
              end, ReplicaNodes),
    coordinate_replicas_recurse(Counter, []).

handle_forward_request(Socket, Host, Port, NodeRequest) ->
    %% send requests to replicas in successor list
    ?P("handle forward request~n"),
    Key = NodeRequest#node_request.client_request#client_request.key,
    Length = get_config(?REPLICATION),
    NodeIPPortPair = {{127, 0, 0, 1}, get_config(?NODE_PORT)},
    node_hash_lookup ! {self(), successor, Key, Length},
    receive
        {successor, Nodes} ->
            ?P("Peers for the requests: ~p~n", [Nodes]),
            ReplicaNodes = lists:filter(fun(X) -> X =/= NodeIPPortPair end, Nodes),
            Responses = coordinate_replicas(ReplicaNodes, NodeRequest),
            Status = request_status(Responses),
            Data = encode_node_request("FORWARD_RESP", Status, Responses),
            gen_udp:send(Socket, Host, Port, Data)
    end.

handle_forward_response(Socket, Host, Port, NodeRequest) ->
    %% Handle forward response
    gen_udp:send(Socket, Host, Port, serialize_node_request(NodeRequest)).

request_status(Responses) ->
    %% Check all the response is success or not.
    case lists:any(fun(Resp) -> Resp =:= {"error"} end, Responses) of
        true ->
            "failure";
        false ->
            "success"
    end.

handle_replica_request(Socket, SourceHost, SourcePort, NodeRequest) ->
    ?P("~p/~p: ~p, ~p, ~p", [?FUNCTION_NAME, ?FUNCTION_ARITY, SourceHost, SourcePort, NodeRequest]),
    {RequestType, {Method, Key, Value}} = unpack_node_request(NodeRequest),

    arbiter ! {self(), Method, Key, Value},
    receive
        {arbiter, ResponseMethod, ResponseKey, ResponseVal} ->
            io:format('Sending Response to node, Key: ~p, Val: ~p~n', [ResponseKey, ResponseVal]),

            % respond to the coordinator
            gen_udp:send(Socket, SourceHost, SourcePort,
                request_pb:encode({noderequest, response_type(RequestType), 
                                   {clientrequest, ResponseMethod, ResponseKey, ResponseVal}}))
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
        {udp, ClientSocket, ClientHost, ClientPort, RawData} ->
            io:format('----------------~n'),
            {_, Method, Key, Value} = request_pb:decode_clientrequest(RawData),
            ClientRequest = #client_request{
                method=Method,
                key=Key,
                value=Value
            },
            io:format("Received data: ~p from `~p` `~p` on `~p`~n", 
                      [ClientRequest, ClientHost, ClientPort, get_config(?CLIENT_PORT)]),
            spawn_link(fun() -> handle_client_request(
                                  ClientSocket, ClientHost, ClientPort, ClientRequest) 
                       end),
            ?P("Socket: ~p, Client Socket: ~p~n", [Socket, ClientSocket]),
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

unpack_client_request_as_tuple(ClientRequest) ->
    Method = ClientRequest#client_request.method,
    Key = ClientRequest#client_request.key,
    Value = ClientRequest#client_request.value,
    {Method, Key, Value}.

handle_local_connection(ClientSocket, ClientHost, ClientPort, ClientRequest) ->
    {Method, Key, Value} = unpack_client_request(ClientRequest),
    case Method of
        "GET" -> spawn_link(fun() -> handle_client_get(
                                                  ClientSocket, ClientHost, ClientPort, Key) end);
        "PUT" -> spawn_link(fun() -> handle_client_put(
                                                  ClientSocket, ClientHost, ClientPort, Key, Value) end);
        true -> spawn_link(fun() -> handle_rogue_msg(
                                      ClientSocket, ClientHost, ClientPort, Method, Key, Value) end)
    end.

handle_remote_connection(ClientSocket, ClientHost, ClientPort, NodeHost, NodePort, RawData) ->
    spawn_link(fun() -> handle_remote_connection_inner(
                          ClientSocket, ClientHost, ClientPort, NodeHost, NodePort, RawData)
               end).

handle_remote_connection_inner(ClientSocket, ClientHost, ClientPort, NodeHost, NodePort, ClientRequest) ->
    % If the port number is 0, OS chooses the port for the connection.
    {ok, NodeSocket} = gen_udp:open(0, [binary]),
    {Method, Key, Value} = unpack_client_request_as_tuple(ClientRequest),
    ForwardRequestData = request_pb:encode({noderequest, "FORWARD_REQ", undefined,
                                            [{clientrequest, Method, Key, Value}]}),
    io:format("Received data in ~p~n", [?FUNCTION_NAME]),
    Res = gen_udp:send(NodeSocket, NodeHost, NodePort, ForwardRequestData),
    ?P("Socket forward request ~p:~p status ~p~n", [NodeHost, NodePort, Res]),
    receive
        {udp, _, _, _, Response} ->
            io:format('Sending data to client: ~p~n', [Response]),
            gen_udp:send(ClientSocket, ClientHost, ClientPort, Response)
    end.

handle_client_request(ClientSocket, ClientHost, ClientPort, ClientRequest) ->
    Key = ClientRequest#client_request.key,
    node_hash_lookup ! {self(), lookup, Key},
    {NodeHost, NodePort} = listen_to_lookup_response(),
    CurNodePort = get_config(?NODE_PORT),
    if
        % TODO: Fix IP
        {NodeHost, NodePort} == {{127, 0, 0, 1}, CurNodePort} ->
            NodeRequest = #node_request{type="FORWARD_REQ", 
                                        client_request=ClientRequest},
            handle_node_request(ClientSocket, ClientHost, ClientPort, NodeRequest);
            %handle_local_connection(ClientSocket, ClientHost, ClientPort, ClientRequest);
        true ->
            handle_remote_connection(
              ClientSocket, ClientHost, ClientPort, NodeHost, NodePort, ClientRequest)
    end.

handle_rogue_msg(Socket, Host, Port, Method, Key, Value) ->
    io:format('rogue'),
    gen_udp:send(Socket, Host, Port, request_pb:encode({clientrequest, Method, Key, Value})).

handle_client_get(Socket, Host, Port, Key) ->
    DBHandle = open_store(),
    BinaryKey = to_binary(Key),
    case bitcask:get(DBHandle, BinaryKey) of
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
    BinaryKey = to_binary(Key),
    BinaryValue = to_binary(Value),
    case bitcask:put(DBHandle, BinaryKey, BinaryValue) of
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
    Res = gen_udp:send(Socket, ServerHost, ServerPort, request_pb:encode(
                                                         {clientrequest, Method, Key, Value})),
    ?P("Send socket rsp: ~p~n", [Res]),
    receive
        {udp, _, ServerHost, ServerPort, Response} ->
            io:format("Response from Host: ~p, Port: ~p, Response: ~p~n",
                      [ServerHost, ServerPort, request_pb:decode_noderequest(Response)])
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
