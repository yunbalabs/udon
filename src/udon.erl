-module(udon).
-include("udon.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-compile([{parse_transform, lager_transform}]).

-export([sadd/2, sadd_with_ttl/3, srem/2, smembers/2, del/2, smembers2/2]).

-ignore_xref([sadd/2, sadd_with_ttl/3, srem/2, smembers/2, del/2, smembers2/2]).

%% Public API

%% @doc Add an item to a set
sadd({Bucket, Key}, Item) ->
    {ok, ReqId} = udon_op_fsm:op(?N, ?W, {sadd, {Bucket, Key}, Item}, {Bucket, Key}),
    case wait_for_reqid(ReqId, ?TIMEOUT) of
        {ok, [ok, ok]} ->
            jiffy:encode({[{<<"status">>, 0}]});
        Error ->
            handle_error(sadd, [{Bucket, Key}, Item], Error)
    end;
sadd(_, _) ->
    jiffy:encode({[{<<"status">>, 1}, {<<"error">>, <<"invalid parameters">>}]}).

sadd_with_ttl({Bucket, Key}, Item, [{"ttl", TTL}]) ->
    CombinedKey = redis_backend:get_combined_key({Bucket, Key}),
    case transaction({Bucket, Key}, [
        [<<"SADD">>, CombinedKey, Item],
        [<<"EXPIRE">>, CombinedKey, TTL]
    ]) of
        {ok, [ok, ok]} ->
            jiffy:encode({[{<<"status">>, 0}]});
        Error ->
            handle_error(sadd, [{Bucket, Key}, Item], Error)
    end;
sadd_with_ttl(_, _, _) ->
    jiffy:encode({[{<<"status">>, 1}, {<<"error">>, <<"invalid parameters">>}]}).

%% @doc Remove an item to a set
srem({Bucket, Key}, Item) ->
    {ok, ReqId} = udon_op_fsm:op(?N, ?W, {srem, {Bucket, Key}, Item}, {Bucket, Key}),
    case wait_for_reqid(ReqId, ?TIMEOUT) of
        {ok, [ok, ok]} ->
            jiffy:encode({[{<<"status">>, 0}]});
        Error ->
            handle_error(srem, [{Bucket, Key}, Item], Error)
    end;
srem(_, _) ->
    jiffy:encode({[{<<"status">>, 1}, {<<"error">>, <<"invalid parameters">>}]}).

%% @doc Delete a set
del(Bucket, Key) ->
    {ok, ReqId} = udon_op_fsm:op(?N, ?W, {del, Bucket, Key}, {Bucket, Key}),
    case wait_for_reqid(ReqId, ?TIMEOUT) of
        {ok, [ok, ok]} ->
            jiffy:encode({[{<<"status">>, 0}]});
        Error ->
            handle_error(srem, [Bucket, Key], Error)
    end;
del(_, _) ->
    jiffy:encode({[{<<"status">>, 1}, {<<"error">>, <<"invalid parameters">>}]}).

%% @doc Fetch items in a set
smembers(Bucket, Key) ->
    {ok, ReqId} = udon_op_fsm:op(?N, ?R, {smembers, Bucket, Key}, {Bucket, Key}),
    case wait_for_reqid(ReqId, ?TIMEOUT) of
        {ok, [{ok, Data1}, {ok, Data2}, {ok, Data3}]} ->
            Data = lists_union([Data1, Data2, Data3]),
            jiffy:encode({[{<<"status">>, 0}, {<<"data">>, Data}]});
        Error ->
            handle_error(smembers, [Bucket, Key], Error)
    end;
smembers(_, _) ->
    jiffy:encode({[{<<"status">>, 1}, {<<"error">>, <<"invalid parameters">>}]}).

smembers2(Bucket, Key) ->
    {ok, ReqId} = udon_op_fsm:op(?N, ?R, {redis_address}, {Bucket, Key}),
    case wait_for_reqid(ReqId, ?TIMEOUT) of
        {ok, RedisAddresses} ->
            WorkerPid = self(),
            lists:foreach(fun({ok,{Hostname, Port}}) ->
                spawn(fun() ->
                    case eredis:start_link(Hostname, list_to_integer(Port)) of
                        {ok, RedisContext} ->
                            CombinedKey = [Bucket, <<",">>, Key],
                            case eredis:q(RedisContext, [<<"SMEMBERS">>, CombinedKey]) of
                                {ok, Value} ->
                                    WorkerPid ! {ok, Value};
                                {error, Reason} ->
                                    WorkerPid ! {error, Reason}
                            end,
                            eredis:stop(RedisContext);
                        {error, Reason} ->
                            WorkerPid ! {error, Reason}
                    end
                end)
            end, RedisAddresses),
            case collect_result(length(RedisAddresses), []) of
                [{ok, Data1}, {ok, Data2}, {ok, Data3}] ->
                    Data = lists_union([Data1, Data2, Data3]),
                    jiffy:encode({[{<<"status">>, 0}, {<<"data">>, Data}]});
                Error ->
                    handle_error(smembers2, [Bucket, Key], Error)
            end;
        Error ->
            handle_error(smembers2, [Bucket, Key], Error)
    end;
smembers2(_, _) ->
    jiffy:encode({[{<<"status">>, 1}, {<<"error">>, <<"invalid parameters">>}]}).

transaction({Bucket, Key}, CommandList) ->
    {ok, ReqId} = udon_op_fsm:op(?N, ?W, {transaction, {Bucket, Key}, CommandList}, {Bucket, Key}),
    wait_for_reqid(ReqId, ?TIMEOUT).

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

wait_for_reqid(Id, Timeout) ->
    receive {Id, Value} -> {ok, Value}
    after Timeout -> {error, timeout}
    end.

handle_error(Operation, Args, Error) ->
    case Error of
        {error, timeout} ->
            lager:error("~p ~p timeout", [Operation, Args]),
            jiffy:encode({[{<<"status">>, 2}, {<<"error">>, <<"operation timeout">>}]});
        _ ->
            lager:error("~p ~p failed: ~p", [Operation, Args, Error]),
            jiffy:encode({[{<<"status">>, 2}, {<<"error">>, <<"operation failed">>}]})
    end.

lists_union(Lists) ->
    lists_union(Lists, gb_sets:new()).

lists_union([], Set) ->
    gb_sets:to_list(Set);
lists_union([List | Rest], Set) ->
    S = gb_sets:from_list(List),
    Set2 = gb_sets:union([S, Set]),
    lists_union(Rest, Set2).