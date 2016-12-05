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
-define(DATADIR, ".bitcaskdata").

open_store() ->
    bitcask:open(?DATADIR, [read_write]).

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


client(Method, Key, Value) ->
    {ok, Socket} = gen_udp:open(8888),
    gen_udp:send(Socket, {127,0,0,1}, 4545, request_pb:encode(
                                              {request, Method, Key, Value})),
    gen_udp:close(Socket).
