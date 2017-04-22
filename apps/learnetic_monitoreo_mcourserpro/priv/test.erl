-module(test).
-export([start/0, query_user/1]).

start() ->
    %code:priv_dir(Application)
    Dir = "/Users/ivandavid77/learnetic_monitoreo_mcourserpro/apps/learnetic_monitoreo_mcourserpro/priv/",
    Cmd = "python_framework/bin/python extract-users.py",
    Opts = [nouse_stdio, exit_status, binary, {packet, 4}, {cd, Dir}],
    Port = open_port({spawn, Cmd}, Opts),
    get_users({Port, []}).

get_users({Port, Users}) ->
    receive
        {Port, {exit_status, 1}} ->
            io:format("Reintentar");
        {Port, {exit_status, 0}} ->
            wait_for_completion(Users);
        {Port, {data, Data}} ->
            get_users({Port, [binary_to_term(Data)|Users]})
    end.


query_users([]) -> ok;
query_users([User|Users]) ->
    spawn(?MODULE, query_user, [User]),
    query_users(Users).

query_user(User) ->
    Dir = "/Users/ivandavid77/learnetic_monitoreo_mcourserpro/apps/learnetic_monitoreo_mcourserpro/priv/",
    Cmd = "python_framework/bin/python extract-bigquery.py "++User,
    Opts = [use_stdio, exit_status, binary, {cd, Dir}],
    Port = open_port({spawn, Cmd}, Opts),
    receive
        {Port, {exit_status, 1}} -> query_user(User);
        {Port, {exit_status, 0}} ->
    end.
