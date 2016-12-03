%%%-------------------------------------------------------------------
%% @doc edht top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(edht_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

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
            % Spawn new Proc to send data back?
            gen_udp:send(Socket, Host, Port, Bin),
            client_loop(Socket)
    end.
