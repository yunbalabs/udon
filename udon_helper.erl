#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name udon_helper@127.0.0.1 -setcookie udon -pz deps/getopt/ebin

-include("src/udon.hrl").

option_spec_list() ->
  [
    {help, $h, "help", undefined, "Show the program options"},
    {node, $n, "node", string, "the node name"},
    {location, $l, "location", string, "location of the node,bucket,key"},
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
  LocationTuple = lists:keyfind(location, 1, Options),
  Handoff = lists:any(fun(E) -> E  =:= handoff end, Options),
  if
    LocationTuple =/= false ->
      {location, BucketKey} = LocationTuple,
      {Bucket, Key} = lists:splitwith(fun(A) -> A =:= "," end, BucketKey),
      get_location(Node, Bucket, Key);
    Handoff =/= false ->
      get_handoff_status(Node)
  end.

get_location(Node, Bucket, Key) ->
  pong = net_adm:ping(Node),
  HashKey = rpc:call(Node, riak_core_util, chash_key, [{Bucket, Key}]),
  io:format("~p~n", [rpc:call(Node, riak_core_apl, get_apl, [HashKey, ?N, udon])]).

get_handoff_status(Node) ->
  pong = net_adm:ping(Node),
  io:format("~p~n", [rpc:call(Node, riak_core_handoff_manager, status, [])]).