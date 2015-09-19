%%%-------------------------------------------------------------------
%%% @author ZhengYinyong
%%% @copyright (C) 2015, Yunba
%%% @doc
%%%
%%% @end
%%% Created : 18. 九月 2015 下午4:12
%%%-------------------------------------------------------------------
-module(udon_api_test).
-author("ZhengYinyong").

-compile(export_all).

-define(TEST_BUCKET,    "UDON_TEST_BUCKET").
-define(TEST_KEY,       "UDON_TEST_KEY").
-define(TEST_KEY_VALUE, "UDON_TEST_KEY_VALUE").
-define(TEST_TTL, 2).

all() ->
    [
          sadd_test
        , sadd_illegal_test
        , sadd_with_ttl_test
        , sadd_with_ttl_illegal_test
        , smembers_test
        , smembers2_test
        , srem_test
    ].

sadd_test(_Config) ->
    ExpectResult = jiffy:encode({[{<<"status">>, 0}]}),
    ExpectResult = udon:sadd({?TEST_BUCKET, ?TEST_KEY}, ?TEST_KEY_VALUE),
    ExpectResult = udon:del(?TEST_BUCKET, ?TEST_KEY).

sadd_illegal_test(_Config) ->
    ExpectResult =  jiffy:encode({[{<<"status">>, 1}, {<<"error">>, <<"invalid parameters">>}]}),
    ExpectResult= udon:sadd(?TEST_BUCKET, ?TEST_KEY).

sadd_with_ttl_test(_Config) ->
    ExpectResult = jiffy:encode({[{<<"status">>, 0}]}),
    ExpectResult = udon:sadd_with_ttl({?TEST_BUCKET, ?TEST_KEY}, ?TEST_KEY_VALUE, [{"ttl", ?TEST_TTL}]),
    ExpectResult = udon:del(?TEST_BUCKET, ?TEST_KEY).

sadd_with_ttl_illegal_test(_Config) ->
    ExpectResult = jiffy:encode({[{<<"status">>, 1}, {<<"error">>, <<"invalid parameters">>}]}),
    ExpectResult= udon:sadd_with_ttl(?TEST_BUCKET, ?TEST_KEY, ?TEST_TTL).

smembers_test(_Config) ->
    ExpectResult1 = jiffy:encode({[{<<"status">>, 0}]}),
    ExpectResult1 = udon:sadd_with_ttl({?TEST_BUCKET, ?TEST_KEY}, ?TEST_KEY_VALUE, [{"ttl", ?TEST_TTL}]),
    ExpectResult2 = jiffy:encode({[{<<"status">>, 0}, {<<"data">>, [list_to_binary(?TEST_KEY_VALUE)]}]}),
    ExpectResult2 = udon:smembers(?TEST_BUCKET, ?TEST_KEY),
    ExpectResult1 = udon:del(?TEST_BUCKET, ?TEST_KEY).

smembers2_test(_Config) ->
    ExpectResult1 = jiffy:encode({[{<<"status">>, 0}]}),
    ExpectResult1 = udon:sadd_with_ttl({?TEST_BUCKET, ?TEST_KEY}, ?TEST_KEY_VALUE, [{"ttl", ?TEST_TTL}]),
    ExpectResult2 = jiffy:encode({[{<<"status">>, 0}, {<<"data">>, [list_to_binary(?TEST_KEY_VALUE)]}]}),
    ExpectResult2 = udon:smembers2(?TEST_BUCKET, ?TEST_KEY),
    ExpectResult1 = udon:del(?TEST_BUCKET, ?TEST_KEY).

srem_test(_Config) ->
    ExpectResult1 = jiffy:encode({[{<<"status">>, 0}]}),
    ExpectResult1 = udon:sadd_with_ttl({?TEST_BUCKET, ?TEST_KEY}, ?TEST_KEY_VALUE, [{"ttl", ?TEST_TTL}]),
    ExpectResult1 = udon:srem({?TEST_BUCKET, ?TEST_KEY}, ?TEST_KEY_VALUE),
    ExpectResult2 = jiffy:encode({[{<<"status">>, 0}, {<<"data">>, []}]}),
    ExpectResult2 = udon:smembers(?TEST_BUCKET, ?TEST_KEY).
