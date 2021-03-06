%%%-------------------------------------------------------------------
%%% @author ivandavid77
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Apr 2017 5:39 PM
%%%-------------------------------------------------------------------
-module(worker_sup).
-author("ivandavid77").

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1, start_worker/1]).

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

start_worker(User) ->
    supervisor:start_child(?SERVER, child(bigquery_worker, User)).


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
        []
    }} |
    ignore |
    {error, Reason :: term()}).
init([]) ->
    {ok, { {one_for_one, 10, 3600}, []} }.

%%%===================================================================
%%% Internal functions
%%%===================================================================

child(Module, User) ->
    Restart = transient,
    Shutdown = 2000,
    Type = worker,
    {User, {Module, start_link, [User]}, Restart, Shutdown, Type, [Module]}.