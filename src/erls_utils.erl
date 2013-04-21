%%%-------------------------------------------------------------------
%%% @author Tristan Sloughter <>
%%% @copyright (C) 2010, Tristan Sloughter
%%% @doc
%%%
%%% @end
%%% Created : 14 Feb 2010 by Tristan Sloughter <>
%%%-------------------------------------------------------------------
-module(erls_utils).
-compile([export_all]).

comma_separate([H | []]) ->
    H;
comma_separate(List) ->
    lists:foldl(fun(String, Acc) ->
                        io_lib:format("~s,~s", [String, Acc])
                end, "", List).

join(L) ->
    string:join(lists:map(fun(X) when is_atom(X)->atom_to_list(X);(X)->X end, L), ",").
    %% L1 = lists:map(fun(X) when is_atom(X) ->
    %%                        atom_to_list(X);
    %%                   (X) ->
    %%                        X
    %%                end, L),
    %% io:fwrite("erls_utils ~p:join/1:L1=~p~n",[?LINE, L1]),
    %% L2 = string:join(L1, ","),
    %% io:fwrite("erls_utils ~p:join/1:L2=~p~n",[?LINE, L2]),
    %% L2.
    
                           
