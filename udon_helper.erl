#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -sname udon_helper -setcookie udon -pz deps/getopt/ebin -pz deps/jiffy/ebin

-include("src/udon.hrl").

option_spec_list() ->
  [
    {help, $h, "help", undefined, "Show the program options"},
    {node, $n, "node", string, "the node name"},
    {location, $l, "location", string, "location of the bucket,key"},
    {set, $s, "set", string, "set value of the bucket,key"},
    {get, $g, "get", string, "get value of the bucket,key"},
    {exist, $e, "exist", string, "Returns true if value exists in the set of bucket,key"},
    {remove, $r, "remove", string, "remove value of the bucket,key"},
    {handoff, $f, "handoff", undefined, "handoff status of the node"}
  ].

usage() ->
  getopt:usage(option_spec_list(), escript:script_name()).

main([]) ->
  usage();

main(Args) ->
  OptSpecList = option_spec_list(),
  case getopt:parse(OptSpecList, Args) of
    {ok, {Options, _NonOptArgs}} ->
      do(Options);
    {error, {Reason, Data}} ->
      io:format("Error: ~s ~p~n~n", [Reason, Data]),
      usage()
  end.

do([]) ->
  ignore;

do(Options) ->
  {node, NodeStr} = lists:keyfind(node, 1, Options),
  Node = list_to_atom(NodeStr),
  pong = net_adm:ping(Node),

  LocationTuple = lists:keyfind(location, 1, Options),
  SetTuple = lists:keyfind(set, 1, Options),
  GetTuple = lists:keyfind(get, 1, Options),
  ExistTuple = lists:keyfind(exist, 1, Options),
  RemoveTuple = lists:keyfind(remove, 1, Options),
  Handoff = lists:any(fun(E) -> E  =:= handoff end, Options),
  if
    LocationTuple =/= false ->
      {location, BucketKey} = LocationTuple,
      [Bucket, Key] = spilt_input(BucketKey),
      get_location(Node, Bucket, Key);
    SetTuple =/= false ->
      {set, BucketKeyValue} = SetTuple,
      [Bucket, Key, Value] = spilt_input(BucketKeyValue),
      set_value(Node, Bucket, Key, Value);
    GetTuple =/= false ->
      {get, BucketKey} = GetTuple,
      [Bucket, Key] = spilt_input(BucketKey),
      get_value(Node, Bucket, Key);
    ExistTuple =/= false ->
      {exist, BucketKeyValue} = ExistTuple,
      [Bucket, Key, Value] = spilt_input(BucketKeyValue),
      exist_value(Node, Bucket, Key, Value);
    RemoveTuple =/= false ->
      {remove, BucketKeyValue} = RemoveTuple,
      [Bucket, Key, Value] = spilt_input(BucketKeyValue),
      remove_value(Node, Bucket, Key, Value);
    Handoff =/= false ->
      get_handoff_status(Node)
  end.

get_location(Node, Bucket, Key) ->
  HashKey = rpc:call(Node, riak_core_util, chash_key, [{Bucket, Key}]),
  io:format("~p,~p: ~p~n", [Bucket, Key, rpc:call(Node, riak_core_apl, get_apl, [HashKey, 1, udon])]).

set_value(Node, Bucket, Key, Value) ->
  io:format("~p,~p: ~p~n", [Bucket, Key, rpc:call(Node, udon, sadd, [{Bucket, Key}, Value])]).

get_value(Node, Bucket, Key) ->
  io:format("~p,~p: ~p~n", [Bucket, Key, rpc:call(Node, udon, smembers, [Bucket, Key])]).

exist_value(Node, Bucket, Key, Value) ->
  Result = rpc:call(Node, udon, smembers2, [Bucket, Key]),
  {Result2} = jiffy:decode(Result),
  Data = proplists:get_value(<<"data">>, Result2),
  io:format("~p~n", [lists:any(fun(E) -> E =:= Value end, Data)]).

remove_value(Node, Bucket, Key, Value) ->
  io:format("~p,~p: ~p~n", [Bucket, Key, rpc:call(Node, udon, srem, [{Bucket, Key}, Value])]).

get_handoff_status(Node) ->
  io:format("~p~n", [rpc:call(Node, riak_core_handoff_manager, status, [])]).

spilt_input(Input) ->
  List = string:tokens(Input, ","),
  lists:map(fun (A) ->
    list_to_binary(A)
  end, List).