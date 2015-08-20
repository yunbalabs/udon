-module(basho_bench_driver_udon).

-export([new/1,
         run/4]).

-include("basho_bench.hrl").

-record(state, {idstr, node, r, w}).

new(Id) ->
    Nodes  = basho_bench_config:get(udon_nodes, ['udon@127.0.0.1']),
    NativeCookie = basho_bench_config:get(udon_nodes_cookie, 'udon'),
    %% riakc_pb_replies sets defaults for R, W, DW and RW.
    %% Each can be overridden separately
    Replies = basho_bench_config:get(udon_replies, 3),
    R = basho_bench_config:get(udon_r, Replies),
    W = basho_bench_config:get(udon_w, Replies),

    %% Choose the node using our ID as a modulus
    TargetNode = lists:nth((Id rem length(Nodes)+1), Nodes),
    ?INFO("Using target node ~p for worker ~p\n", [TargetNode, Id]),

    NativeName = list_to_atom("basho_bench_driver_udon@zy-computer.local"),
    ?INFO("Try to start net_kernel with name ~p\n", [NativeName]),
    case net_kernel:start([NativeName]) of
        {ok, _} ->
            ?INFO("Net kernel started as ~p\n", [node()]);
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            ?FAIL_MSG("Failed to start net_kernel for ~p: ~p\n", [?MODULE, Reason])
    end,

    ?INFO("Set cookie to ~p\n", [NativeCookie]),
    true = erlang:set_cookie(node(), NativeCookie),

    %% Check that we can at least talk to the node
    case net_adm:ping(TargetNode) of
        pang ->
            ?FAIL_MSG("~s requires that you run a udon node.\n",
                [?MODULE]);
        _ ->
            {ok, #state{idstr = integer_to_list(Id), node = TargetNode, r = R, w = W}}
    end.

run(get, KeyGen, _ValueGen, State) ->
    Key = integer_to_list(KeyGen()),
    case rpc:call(State#state.node, udon, smembers, [State#state.idstr, Key]) of
        {ok, [{ok, Value}, {ok, Value2}, {ok, Value3}]} ->
%%             CompareFun = fun (A, B) -> A > B end,
%%             SortedValue = lists:sort(CompareFun, Value),
%%             SortedValue2 = lists:sort(CompareFun, Value2),
%%             SortedValue3 = lists:sort(CompareFun, Value3),
%%             if
%%                 SortedValue == SortedValue2 andalso SortedValue2 == SortedValue3 ->
                    {ok, State};
%%                 true ->
%%                     {error, "not equal", State}
%%             end;
        Error ->
            {error, Error, State}
    end;

run(put, KeyGen, ValueGen, State) ->
    Key = integer_to_list(KeyGen()),
    case rpc:call(State#state.node, udon, sadd, [{State#state.idstr, Key}, ValueGen()]) of
        {ok, [ok, ok]} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;

run(delete, KeyGen, ValueGen, State) ->
    Key = integer_to_list(KeyGen()),
    case rpc:call(State#state.node, udon, srem, [{State#state.idstr, Key}, ValueGen()]) of
        {ok, [ok, ok]} ->
            {ok, State};
        Error ->
            {error, Error, State}
    end;

run(get_handoff, _KeyGen, _ValueGen, State) ->
    ActivePartitions = get_active_transfers(State),
    Keys = get_keys_from_availble_redis(ActivePartitions, State),
    ?INFO("get_handoff partitions: ~p\n", [ActivePartitions]),
    ?INFO("get_handoff keys num: ~p keys: ~p\n", [length(Keys), Keys]),
%%     lists:foreach(fun (Key) ->
%%         [Bucket, Key1] = binary:split(Key, <<",">>),
%%         BucketStr = binary_to_list(Bucket),
%%         Key1Str = binary_to_list(Key1),
%%         case rpc:call(State#state.node, udon, smembers, [BucketStr, Key1Str]) of
%%             {ok, [{ok, _Value}, {ok, _Value2}, {ok, _Value3}]} ->
%%                 ok;
%%             Error ->
%%                 ?INFO("get_handoff failed ~p\n", [Error])
%%         end
%%     end, Keys),
    {ok, State};

run(update_handoff, _KeyGen, ValueGen, State) ->
    ActivePartitions = get_active_transfers(State),
    Keys = get_keys_from_availble_redis(ActivePartitions, State),
    ?INFO("update_handoff keys num: ~p partitions: ~p\n", [length(Keys), ActivePartitions]),
    lists:foreach(fun (Key) ->
        [Bucket, Key1] = binary:split(Key, <<",">>),
        BucketStr = binary_to_list(Bucket),
        Key1Str = binary_to_list(Key1),
        Value = ValueGen(),
        case rpc:call(State#state.node, udon, sadd, [{BucketStr, Key1Str}, Value]) of
            {ok, [ok, ok]} ->
                ?INFO("update_handoff success ~p \nvalue: ~p\n", [{Bucket, Key1}, Value]);
            Error ->
                ?INFO("update_handoff failed ~p\n", [Error])
        end
    end, Keys),
    {ok, State};

run(delete_handoff, _KeyGen, ValueGen, State) ->
    ActivePartitions = get_active_transfers(State),
    Keys = get_keys_from_availble_redis(ActivePartitions, State),
    ?INFO("delete_handoff keys num: ~p partitions: ~p\n", [length(Keys), ActivePartitions]),
    lists:foreach(fun (Key) ->
        [Bucket, Key1] = binary:split(Key, <<",">>),
        BucketStr = binary_to_list(Bucket),
        Key1Str = binary_to_list(Key1),
        Value = ValueGen(),
        case rpc:call(State#state.node, udon, srem, [{BucketStr, Key1Str}, Value]) of
            {ok, [ok, ok]} ->
                ?INFO("delete_handoff success ~p \nvalue: ~p\n", [{Bucket, Key1}, Value]);
            Error ->
                ?INFO("delete_handoff failed ~p\n", [Error])
        end
    end, Keys),
    {ok, State}.

get_active_transfers(State) ->
    ActiveRings = rpc:call(State#state.node, riak_core_handoff_manager, status, []),
    P = lists:map(fun ({status_v2, RingStatus}) ->
        case lists:keyfind(src_partition, 1, RingStatus) of
            {src_partition, undefined} ->
                [];
            {src_partition, Partition} ->
                Partition
        end
    end, ActiveRings),
    lists:flatten(P).

get_keys_from_availble_redis(ActivePartitions, State) ->
    Keys = lists:map(fun (Partition) ->
        ExpectedSocketFile = lists:flatten(["/tmp/redis.sock.", atom_to_list(State#state.node), ".", integer_to_list(Partition)]),
        rpc:call(State#state.node, redis_backend, all_keys, [ExpectedSocketFile])
    end, ActivePartitions),
    lists:flatten(Keys).