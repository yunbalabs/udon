-define(PRINT(Var), io:format("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).
-define(KEY(X), {<<"udon">>, X}).

-define(N, 3).
-define(W, 2).
-define(R, 2).
-define(TIMEOUT, 5000). % millisecs


-record(file, {
        request_path,
        path_md5,
        version = 0,
        csum,
        redirect = false
    }).
