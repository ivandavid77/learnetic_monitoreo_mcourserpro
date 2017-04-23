%%%-------------------------------------------------------------------
%%% @author ivandavid77
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 20. Apr 2017 4:17 PM
%%%-------------------------------------------------------------------
-module(generate_workers).
-author("ivandavid77").

-behaviour(gen_server).

%% API
-export([start_link/0, worker_finished/0]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {port_get_users, pending=[], total_users=[], max_concurrent=15, current=0}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @end
%%--------------------------------------------------------------------
-spec(start_link() ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

worker_finished() ->
    gen_server:cast(?SERVER, worker_finished).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([]) ->
    Dir = code:priv_dir(learnetic_monitoreo_mcourserpro),
    Cmd = "python_framework/bin/python extract-users.py",
    Opts = [nouse_stdio, exit_status, binary, {packet, 4}, {cd, Dir}],
    Port = open_port({spawn, Cmd}, Opts),
    {ok, #state{port_get_users=Port}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call(_Request, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(worker_finished, #state{pending=[], current=Current} = State) ->
    {noreply, State#state{current=Current-1}};
handle_cast(worker_finished, #state{pending=[User|RestOfPending]} = State) ->
    worker_sup:start_worker(User),
    {noreply, State#state{pending=RestOfPending}}.



%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(Info, #state{total_users=TotalUsers , max_concurrent=Max, current=Current, pending=Pending} = State) ->
    Port = State#state.port_get_users,
    case Info of
        {Port, {data, BinaryUser}} ->
            User = binary_to_term(BinaryUser),
            NewTotalUsers = [User|TotalUsers],
            case Current < Max of
                true ->
                    worker_sup:start_worker(User),
                    {noreply, State#state{total_users=NewTotalUsers, current=Current+1}};
                false ->
                    {noreply, State#state{total_users=NewTotalUsers, pending=[User|Pending]}}
            end;
        {Port, {exit_status, 0}} ->
            sync_workers:do_data_collection(TotalUsers),
            {noreply, State};
        {Port, {exit_status, 1}} ->
            {stop, {error, "Not could get active users from database"}, State}
    end.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
