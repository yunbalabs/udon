-module(udon_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    case udon_sup:start_link() of
        {ok, Pid} ->
            ok = riak_core:register([{vnode_module, udon_vnode}]),

            ok = riak_core_ring_events:add_guarded_handler(udon_ring_event_handler, []),
            ok = riak_core_node_watcher_events:add_guarded_handler(udon_node_event_handler, []),
            ok = riak_core_node_watcher:service_up(udon, self()),
            {ok, _} = sidejob:new_resource(udon_op_fsm_sj, sidejob_supervisor, 100),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.
