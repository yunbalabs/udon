-module(udon_op_fsm).
-behavior(gen_fsm).
-include("udon.hrl").

%% API
-export([start_link/8, op/6]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2]).

%% req_id: The request id so the caller can verify the response.
%%
%% sender: The pid of the sender so a reply can be made.
%%
%% prelist: The preflist for the data.
%%
%% num_w: The number of successful write replies.
%%
%% op: must be a two item tuple with the command and the params
-record(state, {req_id :: pos_integer(),
                from :: pid(),
                n :: pos_integer(),
                w :: pos_integer(),
                op,
                % key used to calculate the hash
                key,
                accum,
                preflist :: riak_core_apl:preflist2(),
                num_w = 0 :: non_neg_integer(),
                enable_forward,
                timeout}).

-compile([{parse_transform, lager_transform}]).

%%%===================================================================
%%% API
%%%===================================================================

start_link(EnableForward, ReqID, From, Op, Key, N, W, Timeout) ->
    gen_fsm:start_link(?MODULE, [EnableForward, ReqID, From, Op, Key, N, W, Timeout], []).

op(N, W, Op, Key, Timeout, EnableForward) ->
    ReqID = reqid(),
    case sidejob_supervisor:start_child(udon_op_fsm_sj, gen_fsm, start_link, [udon_op_fsm, [EnableForward, ReqID, self(), Op, Key, N, W, Timeout], []]) of
        {error, overload} ->
            {error, overload};
        {ok, _Pid} ->
            {ok, ReqID}
    end.

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([EnableForward, ReqID, From, Op, Key, N, W, Timeout]) ->
    SD = #state{req_id=ReqID, from=From, n=N, w=W, op=Op, key=Key, accum=[], enable_forward = EnableForward, timeout = Timeout},
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{n=1, key=Key, enable_forward = false, from = From, req_id=ReqID}) ->
    DocIdx = riak_core_util:chash_key(Key),
    Preflist = riak_core_apl:get_apl(DocIdx, 1, udon),
    SelfNode = node(),
    case Preflist of
        [{_, SelfNode}] ->
            SD = SD0#state{preflist=Preflist},
            {next_state, execute, SD, 0};
        [{_, TargetNode}] ->
            From ! {forward_disable, ReqID, TargetNode},
            {stop, normal, SD0}
    end;
prepare(timeout, SD0=#state{n=N, key=Key}) ->
    DocIdx = riak_core_util:chash_key(Key),
    Preflist = riak_core_apl:get_apl(DocIdx, N, udon),
    SD = SD0#state{preflist=Preflist},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{req_id=ReqID, op=Op, preflist=Preflist, timeout = Timeout}) ->
    Command = {ReqID, Op},
    riak_core_vnode_master:command(Preflist, Command, {fsm, undefined, self()},
                                   udon_vnode_master),
    {next_state, waiting, SD0, Timeout}.

%% @doc Wait for W write reqs to respond.
waiting({ReqID, Resp}, SD0=#state{from=From, num_w=NumW0, w=W, accum=Accum, timeout = Timeout}) ->
    NumW = NumW0 + 1,
    NewAccum = [Resp|Accum],
    SD = SD0#state{num_w=NumW, accum=NewAccum},
    if
        NumW =:= W ->
            From ! {ReqID, NewAccum},
            {stop, normal, SD};
        true -> {next_state, waiting, SD, Timeout}
    end;
waiting(timeout, SD0) ->
    lager:warning("waiting for respond timeout: ~p", [SD0]),
    {stop, normal, SD0}.

handle_info(Info, _StateName, StateData) ->
    lager:warning("got unexpected info ~p", [Info]),
    {stop,badmsg,StateData}.

handle_event(Event, _StateName, StateData) ->
    lager:warning("got unexpected event ~p", [Event]),
    {stop,badmsg,StateData}.

handle_sync_event(Event, _From, _StateName, StateData) ->
    lager:warning("got unexpected sync event ~p", [Event]),
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%% Private API

reqid() -> erlang:phash2(os:timestamp()).
