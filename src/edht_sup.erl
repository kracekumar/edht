%%%-------------------------------------------------------------------
%% @doc edht top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(edht_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).
-export([client/3]).

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
    Port = get_port(),
    listen_to_clients(Port),
    {ok, { {one_for_all, 0, 1}, []} }.

%%====================================================================
%% Internal functions
%%====================================================================
get_port() ->
    case os:getenv("PORT") of
        false ->
            4545;
        Value -> list_to_integer(Value)
    end.


listen_to_clients(Port) ->
    %% Listen to all UDP clients.
    spawn_link(fun() -> handle_client_connections(Port) end).


handle_client_connections(Port) ->
    {ok, Socket} = gen_udp:open(Port, [binary, {active, false}]),
    io:format("Listening to port ~p~n", [Port]),
    client_loop(Socket).

client_loop(Socket) ->
    inet:setopts(Socket, [{active, once}]),
    receive
        {udp, Socket, Host, Port, Bin} ->
            io:format("Received data `~p` `~p` from `~p`~n", [Host, Port, Bin]),
            {_, Method, Key, Value} = request_pb:decode_request(Bin),
            spawn_link(fun() -> handle_request(Socket, Host, Port, Method, Key, Value) end),
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
    io:format("Key: ~p~n", [Key]),
    gen_udp:send(Socket, Host, Port, request_pb:encode({request, "GET", Key, undefined})).

handle_put(Socket, Host, Port, Key, Value) ->
    io:format("Key: ~p, Value: ~p ~n", [Key, Value]),
    gen_udp:send(Socket, Host, Port, request_pb:encode({request, "GET", Key, Value})).


client(Method, Key, Value) ->
    {ok, Socket} = gen_udp:open(8888),
    gen_udp:send(Socket, {127,0,0,1}, 4545, request_pb:encode(
                                              {request, Method, Key, Value})),
    gen_udp:close(Socket).
