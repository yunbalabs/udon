%%%-------------------------------------------------------------------
%%% @author thi
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 九月 2015 下午3:27
%%%-------------------------------------------------------------------
-module(udon_redis).
-author("thi").

-behaviour(gen_server).

%% API
-export([start_link/0]).
-export([connect_redis/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
    unix_socket     ::  string(),
    listen_port     ::  integer()
}).

%%%===================================================================
%%% API
%%%===================================================================

-spec(connect_redis() -> {ok, Context :: term()} | {error, Reason :: term()}).
connect_redis() ->
    try
        gen_server:call(?SERVER, connect_redis)
    catch E:T ->
        {error, {E, T}}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    DataRoot = filename:absname(app_helper:get_env(hierdis, data_root)),
    ConfigFile = filename:absname(app_helper:get_env(hierdis, config_file)),
    DataDir = filename:join([DataRoot, node()]),

    ExpectedExecutable = filename:absname(app_helper:get_env(hierdis, executable)),
    ExpectedSocketFile = lists:flatten([app_helper:get_env(hierdis, unixsocket), atom_to_list(node()),
        ".", node()]),

    case filelib:ensure_dir(filename:join([DataDir, dummy])) of
        ok ->
            case check_redis_install(ExpectedExecutable) of
                {ok, RedisExecutable} ->
                    case start_redis(node(), RedisExecutable, ConfigFile, ExpectedSocketFile, DataDir) of
                        {ok, RedisSocket, ListenPort} ->
                            {ok, #state{
                                unix_socket = RedisSocket,
                                listen_port = ListenPort
                            }};
                        {error, Reason} -> {error, Reason}
                    end;
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} -> {error, {Reason, "Failed to create data directories for redis backend."}}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(connect_redis, From, State = #state{unix_socket = UnixSocket}) ->
    case hierdis:connect_unix(UnixSocket) of
        {ok, RedisContext} ->
            case wait_for_redis_loaded(RedisContext, 100, 6000) of
                ok ->
                    lager:info("connect redis ok from request: ~p\n", [From]),
                    {reply, {ok, RedisContext}, State};
                timeout ->
                    {error, wait_for_redis_loaded_timeout}
            end;
        {error, Reason} -> {reply, {error, Reason}, State}
    end;
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(_Request, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @private
check_redis_install(Executable) ->
    case file_exists(Executable) of
        true ->
            {ok, Executable};
        false ->
            {error, {io:format("Failed to locate Redis executable: ~p", [Executable])}}
    end.

%% @private
start_redis(Node, Executable, ConfigFile, SocketFile, DataDir) ->
    case file:change_mode(Executable, 8#00755) of
        ok ->
            case file_exists(SocketFile) of
                true ->
                    {ok, {listen_port, ListenPort}} = read_redis_config(Node),
                    {ok, SocketFile, ListenPort};
                false ->
                    case try_start_redis(Executable, ConfigFile, SocketFile, DataDir, 50) of
                        {ok, SocketFile, ListenPort} ->
                            store_redis_config(Node, {listen_port, ListenPort}),
                            {ok, SocketFile, ListenPort};
                        {error, Reason} ->
                            {error, Reason}
                    end
            end;
        {error, Reason} ->
            {error, Reason}
    end.

try_start_redis(_Executable, _ConfigFile, _SocketFile, _DataDir, 0) ->
    {error, {redis_error, "Redis can't start, all ports are unavailable"}};
try_start_redis(Executable, ConfigFile, SocketFile, DataDir, TryTimes) ->
    random:seed(now()),
    ListenPort = 0, %integer_to_list(10000 + random:uniform(50000)),
    Args = [ConfigFile, "--unixsocket", SocketFile, "--daemonize", "yes", "--port", ListenPort, "--databases", 1024],
    Port = erlang:open_port({spawn_executable, [Executable]}, [{args, Args}, {cd, filename:absname(DataDir)}]),
    receive
        {'EXIT', Port, normal} ->
            case wait_for_file(SocketFile, 100, 50) of
                {ok, File} ->
                    {ok, File, ListenPort};
                _Error ->
                    try_start_redis(Executable, ConfigFile, SocketFile, DataDir, TryTimes - 1)
            end;
        {'EXIT', Port, Reason} ->
            {error, {redis_error, Port, Reason, io:format("Could not start Redis via Erlang port: ~p\n", [Executable])}}
    end.

redis_config_file_name(Node) ->
    lists:concat([Node, "_redis_config.dat"]).

read_redis_config(Node) ->
    case ets:file2tab(redis_config_file_name(Node)) of
        {ok, Tab} ->
            Ret = case ets:lookup(Tab, Node) of
                      [{_, Config}] ->
                          {ok, Config};
                      [] ->
                          {error, not_found}
                  end,
            ets:delete(Tab),
            Ret;
        {error, Reason} ->
            {error, Reason}
    end.

store_redis_config(Node, Config) ->
    Tab2 = case ets:file2tab(redis_config_file_name(Node)) of
               {ok, Tab} ->
                   Tab;
               {error, _Reason} ->
                   ets:new(redis_config_table, [])
           end,
    ets:insert(Tab2, {Node, Config}),
    ets:tab2file(Tab2, redis_config_file_name(Node)),
    ets:delete(Tab2).

wait_for_file(File, Msec, Attempts) when Attempts > 0 ->
    case file_exists(File) of
        true->
            {ok, File};
        false ->
            timer:sleep(Msec),
            wait_for_file(File, Msec, Attempts-1)
    end;
wait_for_file(File, _Msec, Attempts) when Attempts =< 0 ->
    {error, {redis_error, "Redis isn't running, couldn't find: ~p\n", [File]}}.

%% @private
wait_for_redis_loaded(_RedisContext, _Msec, 0) ->
    timeout;
wait_for_redis_loaded(RedisContext, Msec, Attempts) ->
    case hierdis:command(RedisContext, ["PING"]) of
        {ok, <<"PONG">>} ->
            ok;
        _ ->
            timer:sleep(Msec),
            wait_for_redis_loaded(RedisContext, Msec, Attempts-1)
    end.

%% @private
file_exists(Filepath) ->
    case filelib:last_modified(filename:absname(Filepath)) of
        0 ->
            false;
        _ ->
            true
    end.
