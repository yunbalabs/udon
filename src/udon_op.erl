%%%-------------------------------------------------------------------
%%% @author zy
%%% @copyright (C) 2015, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 03. 十一月 2015 3:24 PM
%%%-------------------------------------------------------------------
-module(udon_op).
-author("zy").

%% API
-export([op/6]).

-compile([{parse_transform, lager_transform}]).

op(N, RW, Op, Key, Timeout, true) ->
    ReqID = reqid(),
    DocIdx = riak_core_util:chash_key(Key),
    Preflist = riak_core_apl:get_apl(DocIdx, N, udon),
    execute(ReqID, Op, RW, Preflist, Timeout);
op(_N, RW, Op, Key, Timeout, false) ->
    ReqID = reqid(),
    DocIdx = riak_core_util:chash_key(Key),
    Preflist = riak_core_apl:get_apl(DocIdx, 1, udon),
    SelfNode = node(),
    case Preflist of
        [{_, SelfNode}] ->
            execute(ReqID, Op, RW, Preflist, Timeout);
        [{_, TargetNode}] ->
            {forward_disable, TargetNode}
    end.

execute(ReqID, Op, RW, Preflist, Timeout) ->
    Command = {ReqID, Op},
    riak_core_vnode_master:command(Preflist, Command, {fsm, undefined, self()},
        udon_vnode_master),
    waiting(ReqID, RW, [], Timeout).

waiting(ReqID, 0, Accum, _Timeout) ->
    {ok, Accum};
waiting(ReqID, RW, Accum, Timeout) ->
    receive
        {'$gen_event', {ReqID, Resp}} ->
            waiting(ReqID, RW - 1, [Resp |Accum], Timeout)
    after
        Timeout ->
            lager:warning("waiting for respond timeout: ~p", [ReqID]),
            {timeout}
    end.

reqid() -> erlang:phash2(os:timestamp()).