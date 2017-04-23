%%%-------------------------------------------------------------------
%%% @author ivandavid77
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Apr 2017 6:54 PM
%%%-------------------------------------------------------------------
-module(process_pipeline_sup).
-author("ivandavid77").

-behaviour(supervisor).

%% API
-export([start_link/0, start_process/1, stop_process/1]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_process(Process)->
    supervisor:start_child(?SERVER, worker_child(Process)).

stop_process(Process) ->
    supervisor:terminate_child(?SERVER, Process),
    supervisor:delete_child(?SERVER, Process).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, {SupFlags :: {RestartStrategy :: supervisor:strategy(),
        MaxR :: non_neg_integer(), MaxT :: non_neg_integer()},
        [ChildSpec :: supervisor:child_spec()]
    }} |
    ignore |
    {error, Reason :: term()}).
init([]) ->
    RestartStrategy = rest_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
    ChildSpecList = [worker_child(generate_workers), supervisor_child(worker_sup), worker_child(sync_workers)],
    {ok, {SupFlags, ChildSpecList}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
worker_child(Module) ->
    {Module, {Module, start_link, []}, transient, 2000, worker, [Module]}.

supervisor_child(Module) ->
    {Module, {Module, start_link, []}, transient, 10000, supervisor, [Module]}.