-module(udon).
-include("udon.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
    ping/0
%%     store/2,
%%     rename/2,
%%     fetch/1
    , sadd/2, srem/2, transaction/2, smembers/2, smembers2/2]).

-ignore_xref([
              ping/0
    , sadd/2, srem/2
%%               store/2,
%%               rename/2,
%%               fetch/1
             ]).

%% Public API

%% @doc Add an item to a set
sadd({Bucket, Key}, Item) ->
    {ok, ReqId} = udon_op_fsm:op(?N, ?W, {sadd, {Bucket, Key}, Item}, {Bucket, Key}),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Remove an item to a set
srem({Bucket, Key}, Item) ->
    {ok, ReqId} = udon_op_fsm:op(?N, ?W, {srem, {Bucket, Key}, Item}, {Bucket, Key}),
    wait_for_reqid(ReqId, ?TIMEOUT).

transaction({Bucket, Key}, CommandList) ->
    {ok, ReqId} = udon_op_fsm:op(?N, ?W, {transaction, {Bucket, Key}, CommandList}, {Bucket, Key}),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Fetch items in a set
smembers(Bucket, Key) ->
    {ok, ReqId} = udon_op_fsm:op(?N, ?R, {smembers, Bucket, Key}, {Bucket, Key}),
    wait_for_reqid(ReqId, ?TIMEOUT).

smembers2(Bucket, Key) ->
    {ok, ReqId} = udon_op_fsm:op(?N, ?R, {redis_address}, {Bucket, Key}),
    case wait_for_reqid(ReqId, ?TIMEOUT) of
        {ok, RedisAddresses} ->
            WorkerPid = self(),
            lists:foreach(fun({ok,{Hostname, Port}}) ->
                spawn(fun() ->
                    case hierdis:connect(Hostname, list_to_integer(Port)) of
                        {ok, RedisContext} ->
                            CombinedKey = [Bucket, <<",">>, Key],
                            case hierdis:command(RedisContext, [<<"SMEMBERS">>, CombinedKey]) of
                                {ok, Value} ->
                                    WorkerPid ! {ok, Value};
                                {error, Reason} ->
                                    WorkerPid ! {error, Reason}
                            end;
                        {error, Reason} ->
                            WorkerPid ! {error, Reason}
                    end
                end)
            end, RedisAddresses),
            case collect_result(length(RedisAddresses), []) of
                [{ok, Value}, {ok, Value2}, {ok, Value3}] ->
                    {ok};
                Error ->
                    {error, Error}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

collect_result(0, Results) ->
    Results;
collect_result(Size, Results) ->
    receive
        Result ->
            collect_result(Size - 1, [Result | Results])
    after
        ?TIMEOUT ->
            collect_result(Size - 1, [{timeout} | Results])
    end.

%% @doc Stores a static file at the given path
store({Bucket, Key}, Value) ->

%%     PHash = path_to_hash(Path),
%%     PRec = #file{ request_path = Path, path_md5 = PHash, csum = erlang:adler32(Data) },

%%     {ok, ReqId} = udon_op_fsm:op(N, W, {store, PRec, Data}, ?KEY(PHash)),

    {ok, ReqId} = udon_op_fsm:op(?N, ?W, {store, {Bucket, Key}, Value}, {Bucket, Key}),

    wait_for_reqid(ReqId, ?TIMEOUT).

%% @TODO Handle redirects
store(redirect, Path, NewPath) ->
    PHash = path_to_hash(Path),

    {ok, ReqId} = udon_op_fsm:op(?N, ?W, {redirect, PHash, NewPath}, ?KEY(PHash)),
    wait_for_reqid(ReqId, ?TIMEOUT).

%% @doc Retrieves a static file from the given path
fetch({Bucket, Key}) ->
%%     PHash = path_to_hash(Path),
%%     Idx = riak_core_util:chash_key(?KEY(PHash)),
    Idx = riak_core_util:chash_key({Bucket, Key}),
    %% TODO: Get a preflist with more than one node
    [{Node, _Type}] = riak_core_apl:get_primary_apl(Idx, 1, udon),
    riak_core_vnode_master:sync_spawn_command(Node, {fetch, {Bucket, Key}}, udon_vnode_master).

rename(Path, NewPath) ->
    Data = fetch(Path),
    store(NewPath, Data),
    store(redirect, Path, NewPath).
    
%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, udon),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, udon_vnode_master).

wait_for_reqid(Id, Timeout) ->
    receive {Id, Value} -> {ok, Value}
    after Timeout -> {error, timeout}
    end.

path_to_hash(Path) when is_list(Path) ->
    crypto:hash(md5, Path).

