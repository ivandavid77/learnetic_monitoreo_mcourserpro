%%%-------------------------------------------------------------------
%% @doc learnetic_monitoreo_mcourserpro top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(learnetic_monitoreo_mcourserpro_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

start_process_pipeline() ->
    supervisor:start_child(?SERVER, supervisor_child(process_pipeline_sup)).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    ChildSpecList = [worker_child(sentinel)],
    {ok, { {one_for_one, 10, 3600}, ChildSpecList} }.


%%====================================================================
%% Internal functions
%%====================================================================
worker_child(Module) ->
    {Module, {Module, start_link, []}, permanent, 2000, worker, [Module]}.

supervisor_child(Module) ->
    {Module, {Module, start_link, []}, transient, 10000, supervisor, [Module]}.