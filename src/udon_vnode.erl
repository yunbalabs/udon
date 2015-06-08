-module(udon_vnode).
-behaviour(riak_core_vnode).
-include("udon.hrl").

-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handoff_receive_start/1,
         handoff_receive_finish/1,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([
             start_vnode/1
             ]).

-record(state, {partition, basedir="udon_data", redis_state, handoff_receive=false, handoff_receive_queue}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, RedisState} = redis_backend:start(Partition, <<>>),
    Queue = queue:new(),

    St = #state{ partition=Partition, redis_state = RedisState, handoff_receive_queue = Queue},

    Base = make_base_path(St),
    %% filelib:ensure_dir/1 you make me so sad. You won't make the parents
    %% unless there's a last element which need not exist and will not be
    %% created. Crazytown, baby.
    case filelib:ensure_dir(Base ++ "/dummy") of
        ok -> {ok, St};
        {error, Reason} -> {error, Reason}
    end.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command(Req={RequestId, {sadd, {_Bucket, _Key}, _Item}}, Sender, State=#state{
    handoff_receive=true, handoff_receive_queue=Queue
}) ->
    Queue2 = queue:in({Req, Sender}, Queue),
    {reply, {RequestId, ok}, State#state{handoff_receive_queue=Queue2}};
handle_command({RequestId, {sadd, {Bucket, Key}, Item}}, _Sender, State) ->
    Result = case redis_backend:sadd(Bucket, Key, "_", Item, State#state.redis_state) of
                 {ok, _} ->
                     ok;
                 {error, _, _} ->
                     error
             end,
    {reply, {RequestId, Result}, State};

handle_command(Req={RequestId, {srem, {_Bucket, _Key}, _Item}}, Sender, State=#state{
    handoff_receive=true, handoff_receive_queue=Queue
}) ->
    Queue2 = queue:in({Req, Sender}, Queue),
    {reply, {RequestId, ok}, State#state{handoff_receive_queue=Queue2}};
handle_command({RequestId, {srem, {Bucket, Key}, Item}}, _Sender, State) ->
    Result = case redis_backend:srem(Bucket, Key, "_", Item, State#state.redis_state) of
                 {ok, _} ->
                     ok;
                 {error, _, _} ->
                     error
             end,
    {reply, {RequestId, Result}, State};

handle_command({RequestId, {smembers, Bucket, Key}}, _Sender, State) ->
    Result = case redis_backend:smembers(Bucket, Key, "_", State#state.redis_state) of
                 {ok, Value, _} ->
                     {ok, Value};
                 {error, _, _} ->
                     error
             end,
    {reply, {RequestId, Result}, State};

handle_command({RequestId, {redis_address}}, _Sender, State) ->
    {ok, ListenPort, _} = redis_backend:listen_port(State#state.redis_state),
    HostName = net_adm:localhost(),
    {reply, {RequestId, {ok, {HostName, ListenPort}}}, State};

handle_command(Req={RequestId, {store, {_Bucket, _Key}, _Value}}, Sender, State=#state{
    handoff_receive=true, handoff_receive_queue=Queue
}) ->
    Queue2 = queue:in({Req, Sender}, Queue),
    {reply, {RequestId, ok}, State#state{handoff_receive_queue=Queue2}};
handle_command({RequestId, {store, {Bucket, Key}, Value}}, _Sender, State) ->
    Result = case redis_backend:put(Bucket, Key, "_", Value, State#state.redis_state) of
        {ok, _} ->
            ok;
        {error, _, _} ->
            error
    end,
    {reply, {RequestId, Result}, State};

handle_command({fetch, {Bucket, Key}}, _Sender, State) ->
    Res = case redis_backend:get(Bucket, Key, State#state.redis_state) of
              {ok, Value, _} ->
                  {ok, Value};
              {error, not_found, _} ->
                  not_found;
              {error, Reason, _} ->
                  {error, Reason}
          end,
    {reply, Res, State};

%%     MetaPath = make_metadata_path(State, PHash),
%%     Res = case filelib:is_regular(MetaPath) of
%%         true ->
%%             MD = get_metadata(State, PHash),
%%             get_data(State, MD);
%%         false ->
%%             not_found
%%     end,
%%     {reply, {Res, filename:join([make_base_path(State), make_filename(PHash)])}, State};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

%% The `VisitFun' is riak_core_handoff_sender:visit_item/3
%% visit_item/3 is going to do all of the hard work of taking your serialized 
%% data and pushing it over the wire to the remote node.
%%
%% Acc0 here is the internal state of the handoff. visit_item/3 returns an
%% updated handoff state, so you should use that in your own fold over
%% vnode data elements.
%%
%% The goal here is simple: for each vnode, find all objects, then for
%% each object in a vnode, grab its metadata and the file contents, serialize it
%% using the `encode_handoff_item/2' callback and ship it over to the 
%% remote node.
%%
%% The remote node is going to receive the serialized data using 
%% the `handle_handoff_data/2' function below.
handle_handoff_command(FoldReq = ?FOLD_REQ{foldfun=_VisitFun, acc0=_Acc0}, _Sender, State) ->
    Final = redis_backend:handle_handoff_command(FoldReq, _Sender, State#state.redis_state),
    {reply, Final, State};

handle_handoff_command(Req={RequestId, {sadd, {_Bucket, _Key}, _Item}}, Sender, State) ->
    {reply, {_RequestId, ok}, _State} = handle_command(Req, Sender, State),
    {forward, State};
handle_handoff_command(Req={RequestId, {srem, {_Bucket, _Key}, _Item}}, Sender, State) ->
    {reply, {_RequestId, ok}, _State} = handle_command(Req, Sender, State),
    {forward, State};
handle_handoff_command(Req={RequestId, {store, {_Bucket, _Key}, _Value}}, Sender, State) ->
    {reply, {_RequestId, ok}, _State} = handle_command(Req, Sender, State),
    {forward, State};
handle_handoff_command(Req, Sender, State) ->
    handle_command(Req, Sender, State).

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handoff_receive_start(State) ->
    State#state{handoff_receive=true}.

handoff_receive_finish(State=#state{handoff_receive_queue=Queue}) ->
    State2 = State#state{handoff_receive=false},
    ?PRINT([handoff_receive, queue:len(Queue)]),
    Queue2 = queue:filter(fun ({Req, Sender}) ->    %% Req: sadd, srem, store
        {reply, {_RequestId, ok}, _State} = handle_command(Req, Sender, State2),
        false
    end, Queue),
    State#state{handoff_receive=false, handoff_receive_queue=Queue2}.

handle_handoff_data(Data, State) ->
%%     {Meta, Blob} = binary_to_term(Data),
%%     R = case Meta#file.csum =:= erlang:adler32(Blob) of
%%         true ->
%%             Result = store(State, Meta, Blob),
%%             ?PRINT(Result),
%%             ok;
%%         false ->
%%             {error, file_checksum_differs}
%%     end,
    {{Bucket, Key}, Val} = binary_to_term(Data),
    %% ?PRINT([Bucket, Key, Val]),
    R = case redis_backend:sadd(Bucket, Key, "_", Val, State#state.redis_state) of
        {ok, _} ->
            ok;
        {error, Reason, _} ->
            {error, Reason}

    end,
    {reply, R, State}.

encode_handoff_item(_Key, Data = {_Meta, _File}) ->
    term_to_binary(Data).

is_empty(State) ->
%%     Result = case list_dir(State) of
%%         [] -> true;
%%         {error, _Reason} -> true;
%%         _ -> false
%%     end,
    Result = redis_backend:is_empty(State#state.redis_state),
    {Result, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, State = #state{redis_state = RedisState}) ->
    if
        State == undefined ->
            ignore;
        true ->
            redis_backend:stop(RedisState)
    end,
    ok.

%% Private API

get_all_objects(State) ->
    [ strip_meta(F) || F <- filelib:wildcard("*.meta", make_base_path(State)) ].

strip_meta(Filename) ->
    Index = string:chr(Filename, $.),
    string:substr(Filename, 1, Index - 1).

list_dir(State) ->
    case file:list_dir(make_base_path(State)) of
        {ok, Files} -> Files;
        Other -> Other
    end.

path_from_object(Base, Object, Suffix) ->
    File = Object ++ Suffix,
    filename:join([Base, File]).

get_metadata(State = #state{}, #file{ path_md5 = Hash }) ->
    get_metadata(State, Hash);
get_metadata(State = #state{}, Hash) when is_binary(Hash) ->
    MDPath = make_metadata_path(State, Hash),
    get_metadata(MDPath).

get_metadata(MetaDataPath) ->
    {ok, Data} = file:read_file(MetaDataPath),
    binary_to_term(Data).

get_data(State = #state{}, R = #file{ csum = Csum }) ->
    {ok, Data} = file:read_file(make_versioned_file_path(State, R)),
    case Csum =:= erlang:adler32(Data) of
	true -> {ok, Data};
	false -> {error, file_checksum_differs}
    end.

make_metadata_path(State = #state{}, #file{ path_md5 = Hash }) ->
    make_metadata_path(State, Hash);
make_metadata_path(State = #state{}, Hash) when is_binary(Hash) ->
    filename:join([make_base_path(State), make_metadata_filename(Hash)]).

make_versioned_file_path(State = #state{}, #file{ path_md5 = Hash, version = V} ) ->
    filename:join([make_base_path(State) , make_versioned_filename(Hash, V)]).

make_base_path(#state{partition = P, basedir = Base}) ->
    filename:join([Base, integer_to_list(P)]).

store(State = #state{}, R = #file{ request_path = Path, path_md5 = PHash }, Blob) ->
    ?PRINT([R, Blob]),
    redis_backend:put("yunba", Path, "_", Blob, State#state.redis_state),

    Base = make_base_path(State),
    Res0 = store_meta_file(make_metadata_path(State, R), R),
    Res1 = store_file(make_versioned_file_path(State, R), Blob),
    {Res0, Res1, filename:join([Base, make_filename(PHash)])}.

make_metadata_filename(Hash) when is_binary(Hash) ->
    make_filename(Hash) ++ ".meta".

make_filename(Hash) when is_binary(Hash) ->
    hexstring(Hash).

make_versioned_filename(Hash, Version) when is_integer(Version) andalso is_binary(Hash) ->
    make_filename(Hash) ++ "." ++ integer_to_list(Version).

store_meta_file(Loc, Rec) ->
    Bin = term_to_binary(Rec),
    store_file(Loc, Bin).
    
store_file(Loc, Data) ->
    file:write_file(Loc, Data).

hexstring(<<X:128/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~32.16.0b", [X])).
