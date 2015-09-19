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

%% API
-export([all/0]).

%% Test cases
-export([sadd_test/1]).

-define(TEST_BUCKET,    "UDON_TEST_BUCKET").
-define(TEST_KEY,       "UDON_TEST_KEY").
-define(TEST_KEY_VALUE, "UDON_TEST_KEY_VALUE").

all() ->
    [sadd_test].

sadd_test(_Config) ->
    <<"{\"status\":0}">> = udon:sadd({?TEST_BUCKET, ?TEST_KEY}, ?TEST_KEY_VALUE).
