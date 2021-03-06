%%%-------------------------------------------------------------------
%%% @author Jackie dong <>
%%% @copyright (C) 2010, 2012, Jackie dong
%%% @doc
%%%
%%% @end
%%% Created : 14 June 2012 by Jackie Dong <>
%%%-------------------------------------------------------------------

%% provide high level database operations: CRUD

-module(es_api).
-compile(export_all).

-include_lib("../include/erlastic_search.hrl").

%% the id of ES return is binary, but the id of sending to ES to string

%% this won't work, because record_info must be determined at compile time
%% It can't be determined at runtime.
%% to_record(Type, Id, Source) ->
%%     L = lists:map(fun(id) ->
%%                           Id;
%%                      (X) ->
%%                           proplists:get_value(X, Source)
%%                   end, record_info(fields, Type)),
%%     Record = list_to_tuple([Type, Id|L]).         

%% to_record(Type, Id, Source) ->
%%     Args = lists:map(fun
%%                          (id) ->
%%                              Id;
%%                          (X) ->
%%                              proplists:get_value(list_to_binary(atom_to_list(X)), Source)
%%                      end, boss_record_lib:attribute_names(Type)),
%%     apply(Type, new, Args). 

%% save_record(Index, Record) when is_tuple(Record) ->
%%     Type = element(1, Record),
%%     %%Doc = lists:keydelete(id, 1, Record:attributes()),
%%     Doc = lists:foldr(fun
%%                           ({id,_}, Acc) -> Acc;
%%                           ({K,V}, Acc) ->
%%                               PackedVal = pack_value(V),
%%                               [{K, PackedVal}|Acc]
%%                       end, [], Record:attributes()),
%%     Res = case Record:id() of
%%               id ->
%%                   erlastic_search:index_doc(Index, Type, Doc);
%%               DefinedId when is_list(DefinedId) ->
%%                   erlastic_search:index_doc_with_id(Index, Type, DefinedId, Doc)
%%           end,
%%     case Res of
%%         {ok, Json} ->
%%             Id = proplists:get_value(<<"_id">>, Json),
%%             {ok, Record:set(id, Id)};
%%         _ ->
%%             Res
%%     end.

get_index() ->
    {ok, Index} = application:get_env(boss, db_database),
    Index.

to_record(Type, Id, Source) ->
    Args = lists:map(fun
                         (id) ->
                             {id, Id};
                         (X) ->
                             {X, proplists:get_value(
                                   list_to_binary(
                                     atom_to_list(X)), 
                                   Source, undefined)}
                     end, jackie_records:attribute_names(Type)),
    jackie_records:new(Type, Args). 

save_record(Record) when is_tuple(Record) ->
    save_record(get_index(), Record).

save_record(Index, Record) when is_tuple(Record) ->
    Type = element(1, Record),
    Doc = lists:foldr(fun
                          ({id,_}, Acc) -> Acc;
                          ({K,V}, Acc) ->
                              PackedVal = pack_value(V),
                              [{K, PackedVal}|Acc]
                      end, [], jackie_records:attributes(Record)),
    Res = case jackie_records:get(id, Record) of
              undefined ->
                  erlastic_search:index_doc(Index, Type, Doc);
              DefinedId when is_list(DefinedId) ->
                  erlastic_search:index_doc_with_id(Index, Type, DefinedId, Doc)
          end,
    case Res of
        {ok, Json} ->
            Id = proplists:get_value(<<"_id">>, Json),
            {ok, jackie_records:set([{id, Id}], Record)};
        _ ->
            Res
    end.

update_doc(Id, Record) when is_tuple(Record) ->
    update_doc(get_index(), Id, Record).

update_doc(Index, Id, Record) when is_tuple(Record) -> 
    Type = element(1, Record),
    Doc = lists:foldr(fun
                          ({id,_}, Acc) -> Acc;
                          ({K,V}, Acc) ->
                              PackedVal = pack_value(V),
                              [{K, PackedVal}|Acc]
                      end, [], jackie_records:attributes(Record)),
    Res = erlastic_search:update_doc(Index, Type, Id, Doc),
    case Res of
        {ok, _} ->
            ok;
        _ ->
            Res
    end.

index_doc(Type, Doc) when is_list(Doc) ->
    index_doc(get_index(), Type, Doc).

index_doc(Index, Type, Doc) when is_list(Doc) ->
    Res = erlastic_search:index_doc(Index, Type, Doc),
    case Res of
        {ok, Json} ->
            Id = proplists:get_value(<<"_id">>, Json), 
            {ok, Id};
        _ ->
            Res
    end.

index_doc_with_id(Type, Id, Doc) when is_list(Doc) ->
    index_doc_with_id(get_index(), Type, Id, Doc).

index_doc_with_id(Index, Type, Id, Doc) when is_list(Doc) ->
    Res = erlastic_search:index_doc_with_id(Index, Type, Id, Doc),
    case Res of
        {ok, Json} ->
            Id = proplists:get_value(<<"_id">>, Json), 
            {ok, Id};
        _ ->
            Res
    end.
    
delete_doc(Type, Id) ->
    delete_doc(get_index(), Type, Id).

delete_doc(Index, Type, Id) ->
    Res = erlastic_search:delete_doc(Index, Type, Id),
    case Res of
        {ok, _} ->
            ok;
        _ ->
            Res
    end.

delete_doc_by_query(Type, QueryString) ->
    delete_doc_by_query(get_index(), Type, QueryString).

delete_doc_by_query(Index, Type, QueryString) ->
    Res = erlastic_search:delete_doc_by_query(#erls_params{},
                                              [Index], [Type],
                                              QueryString).

find_id(Type, Id) ->
    find_id(get_index(), Type, Id).

find_id(Index, Type, Id) ->
    Res = erls_resource:get(#erls_params{}, filename:join([Index, Type, Id]), [], [], []),
    case Res of
        {ok, Json} ->
            Is_exits = proplists:get_value(<<"exists">>, Json),
            case Is_exits of
                true ->
                    Source = proplists:get_value(<<"_source">>, Json),
                    {ok, to_record(Type, list_to_binary(Id), Source)};
                _ ->
                    {error, not_exists}
            end;
        _ ->
            Res
    end.

%% Sort like [{Field, asc}, {Field2, desc}]
build_sort_opt(PropList) ->
    L = [ [{Field, [{order, Order}]}] || {Field, Order} <- PropList],
    case L of
        [] ->
            [];
        _ ->
            [{sort, L}]
    end.
    
find(Type, Opts, Search, Search_opts) -> 
    find(get_index(), Type, Opts, Search, Search_opts).

find(Index, Type, Opts, match_all, []) -> 
    Res = erlastic_search:q([Index], [Type], Opts, match_all, []),
    res_to_records(Type, Res);
find(Index, Type, Opts, term, TermPL) ->
    Res = erlastic_search:q([Index], [Type], Opts, term, TermPL),
    res_to_records(Type, Res);
find(Index, Type, Opts, query_string, SearchString) -> 
    Res = erlastic_search:q([Index], [Type], Opts, query_string, 
                            [{<<"query">>, SearchString}]),
    res_to_records(Type, Res).

%% for test, currently, we encode date to UTC string instead of long(UTC milliseconds)
pack_value({_, _, _} = Now) ->
    Seconds = date_util:now_to_seconds(Now),
    Date = date_util:seconds_to_utc_time(Seconds),
    list_to_binary(date_format:format(Date, "c"));
pack_value(V) ->
    V.

res_to_records(Type, {ok, Json}) ->
    Hits = erlastic_search:get_hits_data(Json),
    Records = lists:foldl(fun(X, L)->
                                  Id = proplists:get_value(<<"_id">>, X),
                                  Source = proplists:get_value(<<"_source">>, X),
                                  [to_record(Type, Id, Source)|L]
                          end, [], Hits),
    {ok, lists:reverse(Records)};
res_to_records(_, Res) ->
    Res.

%% statistics/facets


