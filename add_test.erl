#!/usr/bin/env escript
%% -*- erlang -*-
%%! -smp enable -name udon_test@127.0.0.1 -setcookie udon -mnesia debug verbose

sadd_test(Node, Bucket, Key, 1) ->
    rpc:call(Node, udon, sadd, [{Bucket, Key}, <<"1">>]);
sadd_test(Node, Bucket, Key, N) ->
    rpc:call(Node, udon, sadd, [{Bucket, Key}, integer_to_binary(N)]),
    sadd_test(Node, Bucket, Key, N-1).

bucket_sadd_test(Node, 1) ->
    sadd_test(Node, "tfs1", "t1", 1000);
bucket_sadd_test(Node, N) ->
    sadd_test(Node, string:concat("tfs", integer_to_list(N)), "t1", 1000),
    bucket_sadd_test(Node, N-1).

main([N]) ->
    {ok, Hostname} = inet:gethostname(),
    Sname = "udon1",
    Node = list_to_atom(lists:concat([Sname, "@", "127.0.0.1"])),
    io:format("~p~n", [Node]),
    pong = net_adm:ping(Node),

    io:format("route table of uid info: ~p~n", [bucket_sadd_test(Node, list_to_integer(N))]).
