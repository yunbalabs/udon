%%%-------------------------------------------------------------------
%%% @author ZhengYinyong
%%% @copyright (C) 2015, Yunba
%%% @doc
%%%
%%% @end
%%% Created : 18. 九月 2015 下午4:10
%%%-------------------------------------------------------------------
-module(udon_SUITE).
-author("ZhengYinyong").

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

init_per_suite(Config) ->
    {ok, _} = application:ensure_all_started(udon),
    Config.

init_per_group(_GroupName, Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

end_per_group(_GroupName, Config) ->
    Config.

all() ->
    [
        {group, udon_api}
    ].

groups() ->
    [
        {udon_api, [], [{udon_api_test, all}]}
    ].
