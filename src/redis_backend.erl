%%%-------------------------------------------------------------------
%%% @author zhanghu
%%% @copyright (C) 2015, yunba.io

% Permission is hereby granted, free of charge, to any person obtaining
% a copy of this software and associated documentation files (the
% 'Software'), to deal in the Software without restriction, including
% without limitation the rights to use, copy, modify, merge, publish,
% distribute, sublicense, and/or sell copies of the Software, and to
% permit persons to whom the Software is furnished to do so, subject to
% the following conditions:

% The above copyright notice and this permission notice shall be
% included in all copies or substantial portions of the Software.

% THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
% EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
% MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
% IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
% CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
% TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
% SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

%%% @doc
%%%
%%% @end
%%% Created : 07. 四月 2015 下午3:28
%%%-------------------------------------------------------------------
-module(redis_backend).
-author("zhanghu").

-include("udon.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

%% Riak Storage Backend API
-export([api_version/0,
    capabilities/1,
    capabilities/2,
    start/2,
    stop/1,
    get/3,
    put/5,
    delete/4,
    drop/1,
    fold_buckets/4,
    fold_keys/4,
    fold_objects/4,
    is_empty/1,
    status/1,
    callback/3, handle_handoff_command/3, sadd/5, srem/5, transaction/2, smembers/4, del/3, listen_port/1,
    all_keys/1, all_values/2, get_combined_key/1]).

-export([data_size/1]).

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold, size]).

-record(state, {redis_context :: term(),
                redis_socket_path :: string(),
                redis_listen_port :: string(),
                storage_scheme :: atom(),
                data_dir :: string(),
                partition :: integer(),
                root :: string()}).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].

-compile([{parse_transform, lager_transform}]).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API and a capabilities list.
%% The current valid capabilities are async_fold
%% and indexes.
-spec api_version() -> {integer(), [atom()]}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start the redis backend
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, _Config) ->
    %% Start the hierdis application.
    case start_hierdis_application() of
        ok ->
            %% Get the data root directory
            DataRoot = filename:absname(app_helper:get_env(hierdis, data_root)),
            ConfigFile = filename:absname(app_helper:get_env(hierdis, config_file)),
            DataDir = filename:join([DataRoot, integer_to_list(Partition)]),

            ExpectedExecutable = filename:absname(app_helper:get_env(hierdis, executable)),
            ExpectedSocketFile = lists:flatten([app_helper:get_env(hierdis, unixsocket), atom_to_list(node()),
                ".", integer_to_list(Partition)]),

            Scheme = case app_helper:get_env(hierdis, storage_scheme) of
                         hash -> hash;
                         _ -> kv
                     end,

            case filelib:ensure_dir(filename:join([DataDir, dummy])) of
                ok ->
                    case check_redis_install(ExpectedExecutable) of
                        {ok, RedisExecutable} ->
                            case start_redis(Partition, RedisExecutable, ConfigFile, ExpectedSocketFile, DataDir) of
                                {ok, RedisSocket, ListenPort} ->
                                    case hierdis:connect_unix(RedisSocket) of
                                        {ok, RedisContext} ->
                                            case wait_for_redis_loaded(RedisContext, 100, 6000) of
                                                ok ->
                                                    Result = {ok, #state{
                                                        redis_context=RedisContext,
                                                        redis_socket_path=RedisSocket,
                                                        redis_listen_port = ListenPort,
                                                        storage_scheme=Scheme,
                                                        data_dir=DataDir,
                                                        partition=Partition,
                                                        root=DataRoot
                                                    }},
                                                    lager:info("Started redis backend for partition: ~p\n", [Partition]),
                                                    Result;
                                                timeout ->
                                                    {error, wait_for_redis_loaded_timeout}
                                            end;
                                        {error, Reason} -> {error, Reason}
                                    end;
                                {error, Reason} -> {error, Reason}
                            end;
                        {error, Reason} -> {error, Reason}
                    end;
                {error, Reason} -> {error, {Reason, "Failed to create data directories for redis backend."}}
            end;
        {error, Reason} -> {error, Reason}
    end.

%% @doc Stop the backend
-spec stop(state()) -> ok.
stop(#state{redis_context=Context}=State) ->
    case app_helper:get_env(hierdis, leave_running) of
        true ->
            ok;
        _ ->
            lager:info("Shutdown Redis ~p", [State#state.partition]),
            case hierdis:command(Context, [<<"SHUTDOWN">>]) of
                {error,{redis_err_eof,"Server closed the connection"}} ->
                    ok;
                {error, Reason} ->
                    {error, Reason, State}
            end
    end.

%% @doc Retrieve an object from the backend
-spec get(riak_object:bucket(), riak_object:key(), state()) -> {ok, any(), state()} | {ok, not_found, state()} | {error, term(), state()}.
get(Bucket, Key, #state{storage_scheme=Scheme}=State) ->
    get(Scheme, Bucket, Key, State).

%% @private
get(hash, Bucket, Key, #state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"HGET">>, Bucket, Key]) of
        {ok, undefined}  ->
            {error, not_found, State};
        {ok, Value} ->
            {ok, Value, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
get(kv, Bucket, Key, #state{redis_context=Context}=State) ->
    CombinedKey = [Bucket, <<",">>, Key],
    case hierdis:command(Context, [<<"GET">>, CombinedKey]) of
        {ok, undefined}  ->
            {error, not_found, State};
        {ok, Value} ->
            {ok, Value, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc sadd to backend
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec sadd(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) -> {ok, state()} | {error, term(), state()}.
sadd(Bucket, Key, _IndexSpec, ItemList, #state{storage_scheme = _Scheme, redis_context = Context} = State)
    when is_list(ItemList) ->
    CombinedKey = [Bucket, <<",">>, Key],
    Command = [<<"SADD">>, CombinedKey] ++ ItemList,
    case hierdis:command(Context, Command) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;

sadd(Bucket, Key, _IndexSpec, Item, #state{storage_scheme = _Scheme, redis_context = Context} = State) ->
    CombinedKey = [Bucket, <<",">>, Key],
    case hierdis:command(Context, [<<"SADD">>, CombinedKey, Item]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

srem(Bucket, Key, _IndexSpec, Item, #state{storage_scheme = _Scheme, redis_context = Context} = State) ->
    CombinedKey = [Bucket, <<",">>, Key],
    case hierdis:command(Context, [<<"SREM">>, CombinedKey, Item]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

transaction(CommandList, #state{storage_scheme = _Scheme, redis_context = Context} = State) ->
    case hierdis:transaction(Context, CommandList) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

smembers(Bucket, Key, _IndexSpec, #state{storage_scheme = _Scheme, redis_context = Context} = State) ->
    CombinedKey = [Bucket, <<",">>, Key],
    case hierdis:command(Context, [<<"SMEMBERS">>, CombinedKey]) of
        {ok, Value} ->
            {ok, Value, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

del(Bucket, Key, #state{storage_scheme = _Scheme, redis_context = Context} = State) ->
    CombinedKey = [Bucket, <<",">>, Key],
    case hierdis:command(Context, [<<"DEL">>, CombinedKey]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

listen_port(#state{redis_listen_port = ListenPort} = State) ->
    {ok, ListenPort, State}.

all_keys(SocketFile) ->
    case file_exists(SocketFile) of
        true ->
            case hierdis:connect_unix(SocketFile) of
                {ok, RedisContext} ->
                    case hierdis:command(RedisContext, [<<"KEYS">>, <<"*">>]) of
                        {ok, Response} ->
                            Response;
                        {error, _Reason} ->
                            []
                    end;
                {error, _Reason} ->
                    []
            end;
        _ ->
            []
    end.

all_values(SocketFile, {Bucket, Key}) ->
    case file_exists(SocketFile) of
        true ->
            case hierdis:connect_unix(SocketFile) of
                {ok, RedisContext} ->
                    CombinedKey = [Bucket, <<",">>, Key],
                    hierdis:command(RedisContext, [<<"SMEMBERS">>, CombinedKey]);
                {error, Reason} ->
                    {error, Reason}
            end;
        _ ->
            {error, file_not_exist}
    end.

%% @doc Insert an object into the backend.
%% -type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) -> {ok, state()} | {error, term(), state()}.
put(Bucket, Key, _IndexSpec, Value, #state{storage_scheme=Scheme}=State) ->
    put(Scheme, Bucket, Key, _IndexSpec, Value, State).

%% @private
put(hash, Bucket, Key, _IndexSpec, Value, #state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"HSET">>, Bucket, Key, Value]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
put(kv, Bucket, Key, _IndexSpec, Value, #state{redis_context=Context}=State) ->
    CombinedKey = [Bucket, <<",">>, Key],
    case hierdis:command(Context, [<<"SET">>, CombinedKey, Value]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Delete an object from the backend
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) -> {ok, state()} | {error, term(), state()}.
delete(Bucket, Key, _IndexSpec, #state{storage_scheme=Scheme}=State) ->
    delete(Scheme, Bucket, Key, _IndexSpec, State).

%% @private
delete(hash, Bucket, Key, _IndexSpec, #state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"HDEL">>, Bucket, Key]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end;
delete(kv, Bucket, Key, _IndexSpec, #state{redis_context=Context}=State) ->
    CombinedKey = [Bucket, <<",">>, Key],
    case hierdis:command(Context, [<<"DEL">>, CombinedKey]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% The `VisitFun' is riak_core_handoff_sender:visit_item/3
%% visit_item/3 is going to do all of the hard work of taking your serialized
%% data and pushing it over the wire to the remote node.
%%
%% Acc0 here is the internal state of the handoff. visit_item/3 returns an
%% updated handoff state, so you should use that in your own fold over
%% vnode data elements.
%%
%% The goal here is simple: for each vnode, find all objects, then for
%% each object in a vnode, grab its metadata and the file contents, serialize it
%% using the `encode_handoff_item/2' callback and ship it over to the
%% remote node.
%%
%% The remote node is going to receive the serialized data using
%% the `handle_handoff_data/2' function below.
handle_handoff_command(?FOLD_REQ{foldfun=VisitFun, acc0=Acc0}, _Sender,
    #state{ redis_context = Context } = State) ->

    FoldFun = fun(Key, AccIn) ->
        {ok, Val} = hierdis:command(Context, ["SMEMBERS", Key]),
        [Bucket, Key1] = binary:split(Key, <<",">>),
        VisitFun(Key, {{Bucket, Key1}, Val}, AccIn)
        end,

    case hierdis:command(Context, [<<"KEYS">>, <<"*">>]) of
        {ok, Response} ->
            AccOut = lists:foldl(FoldFun, Acc0, Response),
            AccOut;
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Fold over all the buckets
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(), any(), [], state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldBucketsFun, Acc, Opts, #state{storage_scheme=Scheme,redis_context=Context}=State) ->
    FoldFun = fold_buckets_fun(Scheme, FoldBucketsFun),
    BucketFolder =
        fun() ->
            case hierdis:command(Context, [<<"KEYS">>, <<"*">>]) of
                {ok, Response} ->
                    {BucketList, _} = lists:foldl(FoldFun, {Acc, sets:new()}, Response),
                    BucketList;
                {error, Reason} -> 
                    {error, Reason, State}
            end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, BucketFolder};
        false ->
            {ok, BucketFolder()}
    end.

%% @private
fold_buckets_fun(hash, FoldBucketsFun) ->
    fun(B, {Acc, _}) ->
        {FoldBucketsFun(B, Acc), undefined}
    end;
fold_buckets_fun(kv, FoldBucketsFun) ->
    fun(CombinedKey, {Acc, BucketSet}) ->
        [B, _Key] = binary:split(CombinedKey, <<",">>),
        case sets:is_element(B, BucketSet) of
            true ->
                {Acc, BucketSet};
            false ->
                {FoldBucketsFun(B, Acc),
                 sets:add_element(B, BucketSet)}
        end
    end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(), any(), [{atom(), term()}], state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, #state{storage_scheme=Scheme}=State) ->
    fold_keys(Scheme, FoldKeysFun, Acc, Opts, State).

%% @private
fold_keys(hash, FoldKeysFun, Acc, Opts, #state{redis_context=Context}=State) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_keys_fun(hash, FoldKeysFun, Bucket),
    KeyFolder =
        fun() ->
            case hierdis:command(Context, [<<"HKEYS">>, Bucket]) of
                {ok, Response} ->
                    KeyList = lists:foldl(FoldFun, Acc, Response),
                    KeyList;
                {error, Reason} -> 
                    {error, Reason, State}
            end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, KeyFolder};
        false ->
            {ok, KeyFolder()}
    end;
fold_keys(kv, FoldKeysFun, Acc, Opts, #state{redis_context=Context}=State) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_keys_fun(kv, FoldKeysFun, Bucket),
    KeyFolder =
        fun() ->
            case hierdis:command(Context, [<<"KEYS">>, [Bucket,",*"]]) of
                {ok, Response} ->
                    KeyList = lists:foldl(FoldFun, Acc, Response),
                    KeyList;
                {error, Reason} -> 
                    {error, Reason, State}
            end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, KeyFolder};
        false ->
            {ok, KeyFolder()}
    end.

%% @private
fold_keys_fun(hash, FoldKeysFun, Bucket) ->
    fun(Key, Acc) ->
        FoldKeysFun(Bucket, Key, Acc)
    end;
fold_keys_fun(kv, FoldKeysFun, _Bucket) ->
    fun(CombinedKey, Acc) ->
        [B, Key] = binary:split(CombinedKey, <<",">>),
        FoldKeysFun(B, Key, Acc)
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(), any(), [{atom(), term()}], state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, #state{storage_scheme=Scheme}=State) ->
    fold_objects(Scheme, FoldObjectsFun, Acc, Opts, State).
 
%% @private
fold_objects(hash, FoldObjectsFun, Acc, Opts, #state{redis_context=Context}=State) ->   
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(hash, FoldObjectsFun, Bucket, Context),
    ObjectFolder =
        fun() ->
            case hierdis:command(Context, [<<"HKEYS">>, Bucket]) of
                {ok, Response} ->
                    ObjectList = lists:foldl(FoldFun, Acc, Response),
                    ObjectList;
                {error, Reason} -> 
                    {error, Reason, State}
            end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, ObjectFolder};
        false ->
            {ok, ObjectFolder()}
    end;
fold_objects(kv, FoldObjectsFun, Acc, Opts, #state{redis_context=Context}=State) ->   
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(kv, FoldObjectsFun, Bucket, Context),
    ObjectFolder =
        fun() ->
            case hierdis:command(Context, [<<"KEYS">>, [Bucket,",*"]]) of
                {ok, Response} ->
                    ObjectList = lists:foldl(FoldFun, Acc, Response),
                    ObjectList;
                {error, Reason} -> 
                    {error, Reason, State}
            end
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, ObjectFolder};
        false ->
            {ok, ObjectFolder()}
    end.

%% @private
fold_objects_fun(hash, FoldObjectsFun, Bucket, RedisContext) ->
    fun(Key, Acc) ->
        case hierdis:command(RedisContext, [<<"HGET">>, Bucket, Key]) of
            {ok, undefined}  ->
                Acc;
            {ok, Value} ->
                FoldObjectsFun(Bucket, Key, Value, Acc);
            {error, _Reason} ->
                Acc
        end
    end;
fold_objects_fun(kv, FoldObjectsFun, _Bucket, RedisContext) ->
    fun(CombinedKey, Acc) ->
        case hierdis:command(RedisContext, [<<"GET">>, CombinedKey]) of
            {ok, undefined}  ->
                Acc;
            {ok, Value} ->
                [B, Key] = binary:split(CombinedKey, <<",">>),
                FoldObjectsFun(B, Key, Value, Acc);
            {error, _Reason} ->
                Acc
        end
    end.

%% @doc Delete all objects from this backend
-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(#state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"FLUSHDB">>]) of
        {ok, _Response} ->
            {ok, State};
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Get the size of the redis database in bytes
-spec data_size(state()) -> undefined | {non_neg_integer(), bytes}.
data_size(#state{redis_context=Context}=_State) ->
    case hierdis:command(Context, [<<"INFO">>, <<"memory">>]) of
        {ok, _Response} ->
            [_JunkHeader | [UsedMemoryElm | _Rest]] = binary:split(_Response, <<"\r\n">>, [global, trim]),
            [_Name | Value] = binary:split(UsedMemoryElm, <<":">>, [global, trim]),
            list_to_integer(binary_to_list(list_to_binary(Value)));
        _ ->
            undefined
    end.

%% @doc Returns true if this backend contains any keys otherwise returns false.
-spec is_empty(state()) -> boolean() | {error, term()}.
is_empty(#state{redis_context=Context}=State) ->
    case hierdis:command(Context, [<<"DBSIZE">>]) of
        {ok, 0} ->
            true;
        {ok, _} ->
            false;
        {error, {redis_err_io, _}} ->   %% redis has shutdown
            true;
        {error, Reason} ->
            {error, Reason, State}
    end.

%% @doc Get the status information for this backend
-spec status(state()) -> [{atom(), term()}].
status(State) ->
    [{state, State}].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Msg, State) ->
    {ok, State}.

%% @private
start_hierdis_application() ->
    case application:start(hierdis) of
        ok ->
            ok;
        {error, {already_started, hierdis}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @private
file_exists(Filepath) ->
    case filelib:last_modified(filename:absname(Filepath)) of
        0 ->
            false;
        _ ->
            true
    end.

%% @private
check_redis_install(Executable) ->
    case file_exists(Executable) of
        true ->
            {ok, Executable};
        false ->
            {error, {io:format("Failed to locate Redis executable: ~p", [Executable])}}
    end.

%% @private
start_redis(Partition, Executable, ConfigFile, SocketFile, DataDir) ->
    case file:change_mode(Executable, 8#00755) of
        ok ->
            case file_exists(SocketFile) of
                true ->
                    {ok, {listen_port, ListenPort}} = read_redis_config(Partition),
                    {ok, SocketFile, ListenPort};
                false ->
                    case try_start_redis(Executable, ConfigFile, SocketFile, DataDir, 50) of
                        {ok, SocketFile, ListenPort} ->
                            store_redis_config(Partition, {listen_port, ListenPort}),
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
    ListenPort = integer_to_list(10000 + random:uniform(50000)),
    Args = [ConfigFile, "--unixsocket", SocketFile, "--daemonize", "yes", "--port", ListenPort],
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

%% @private
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

read_redis_config(Partition) ->
    case ets:file2tab("redis_config.dat") of
        {ok, Tab} ->
            Ret = case ets:lookup(Tab, Partition) of
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

store_redis_config(Partition, Config) ->
    Tab2 = case ets:file2tab("redis_config.dat") of
        {ok, Tab} ->
            Tab;
        {error, _Reason} ->
            ets:new(redis_config_table, [])
    end,
    ets:insert(Tab2, {Partition, Config}),
    ets:tab2file(Tab2, "redis_config.dat"),
    ets:delete(Tab2).

get_combined_key({Bucket, Key}) ->
    [Bucket, <<",">>, Key].