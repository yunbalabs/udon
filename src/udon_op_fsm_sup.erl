%% @doc Supervise the udon_op FSM.
-module(udon_op_fsm_sup).
-behavior(supervisor).

-export([start_write_fsm/1,
    start_link/0]).
-export([init/1]).

start_write_fsm(Args) ->
    supervisor:start_child(?MODULE, Args).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, App}  = application:get_application(?MODULE),
    EnableForward = application:get_env(App, enable_forward, true),

    WriteFsm = {undefined,
                {udon_op_fsm, start_link, [EnableForward]},
                temporary, 5000, worker, [udon_op_fsm]},
    {ok, {{simple_one_for_one, 10, 10}, [WriteFsm]}}.

