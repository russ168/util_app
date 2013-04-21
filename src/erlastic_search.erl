%%%-------------------------------------------------------------------
%%% @author Tristan Sloughter <>
%%% @copyright (C) 2010, 2012, Tristan Sloughter
%%% @doc
%%%
%%% @end
%%% Created : 14 Feb 2010 by Tristan Sloughter <>
%%%-------------------------------------------------------------------
-module(erlastic_search).
-compile(export_all).

-include_lib("../include/erlastic_search.hrl").

%%--------------------------------------------------------------------
%% @doc
%% Takes the name of an index to create and sends the request to
%% Elastic Search, the default settings on localhost.
%%
%% @spec create_index(Index) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
create_index(Index) ->
    create_index(#erls_params{}, Index).

%%--------------------------------------------------------------------
%% @doc
%% Takes the name of an index and the record describing the servers
%% details to create and sends the request to Elastic Search.
%%
%% @spec create_index(Params, Index) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
create_index(Params, Index) ->
    erls_resource:put(Params, Index, [], [], [], []).

%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a Json document described in
%% Erlang terms, converts the document to a string and passes to the
%% default server. Elastic Search provides the doc with an id.
%%
%% @spec index(Index, Type, Doc) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
index_doc(Index, Type, Doc) when is_list(Doc) ->
    index_doc(#erls_params{}, Index, Type, Doc).

%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a Json document described in
%% Erlang terms, converts the document to a string and passes to the
%% server. Elastic Search provides the doc with an id.
%%
%% @spec index(Params Index, Type, Doc) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
index_doc(Params, Index, Type, Doc) when is_list(Doc) ->
    Json = jsonerl:encode(Doc),
    erls_resource:post(Params, filename:join(Index, Type), [], [], Json, []).

%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a Json document described in
%% Erlang terms, converts the document to a string after adding the _id field
%% and passes to the default server.
%%
%% @spec index(Index, Type, Id, Doc) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
index_doc_with_id(Index, Type, Id, Doc) when is_list(Doc) ->
    index_doc_with_id(#erls_params{}, Index, Type, Id, Doc).

%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a Json document described in
%% Erlang terms, converts the document to a string after adding the _id field
%% and passes to the server.
%%
%% @spec index(Params, Index, Type, Id, Doc) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
index_doc_with_id(Params, Index, Type, Id, Doc) when is_list(Doc) ->
    Json = iolist_to_binary(jsonerl:encode(Doc)),
    index_doc_with_id(Params, Index, Type, Id, Json);

index_doc_with_id(Params, Index, Type, Id, Json) when is_binary(Json) ->
    index_doc_with_id_opts(Params, Index, Type, Id, Json, []).

index_doc_with_id_opts(Params, Index, Type, Id, Json, Opts) when is_binary(Json), is_list(Opts) ->
    erls_resource:post(Params, filename:join([Index, Type, Id]), [], Opts, Json, []).

update_doc(Index, Type, Id, Doc) when is_list(Doc) ->
    update_doc(#erls_params{}, Index, Type, Id, Doc).

update_doc(Params, Index, Type, Id, Doc) when is_list(Doc) ->
    Json = iolist_to_binary(jsonerl:encode(Doc)),
    update_doc(Params, Index, Type, Id, Json);

update_doc(Params, Index, Type, Id, Json) when is_binary(Json) ->
    erls_resource:post(
      Params, 
      filename:join([Index, Type, Id, "_update"]), 
      [], [], Json, []).

to_bin(A) when is_atom(A)   -> to_bin(atom_to_list(A));
to_bin(L) when is_list(L)   -> list_to_binary(L);
to_bin(B) when is_binary(B) -> B.

bulk_index_docs(Index, Type, IdJsonLists) ->
    bulk_index_docs(#erls_params{}, Index, Type, IdJsonLists).

bulk_index_docs(Params, Index, Type, IdJsonLists) ->
    Body = lists:foldl(fun({Id, Json}, L) ->
                               Header = jsonerl:encode([{<<"index">>, [{<<"_id">>, to_bin(Id)}]}]),
                             L++[Header, <<"\n">>, jsonerl:encode(Json), <<"\n">>];
                        (Json, L) ->
                             Header = jsonerl:encode([{<<"index">>, []}]),
                               L++[Header, <<"\n">>, jsonerl:encode(Json), <<"\n">>]
                     end, [],
                     IdJsonLists),
    erls_resource:post(Params, filename:join([Index, Type, "_bulk"]), [], [], Body, []).

bulk_index_docs(IndexTypeIdJsonLists) ->
    bulk_index_docs(#erls_params{}, IndexTypeIdJsonLists).

bulk_index_docs(Params, IndexTypeIdJsonLists) ->
    Body = lists:foldl(fun({Index, Type, Id, Json}, L) ->
                             Header = jsonerl:encode([{<<"index">>, [{<<"_index">>, to_bin(Index)},
                                                                     {<<"_type">>, to_bin(Type)},
                                                                     {<<"_id">>, to_bin(Id)}]}]),
                             L++[Header, <<"\n">>, jsonerl:encode(Json), <<"\n">>];
                        ({Index, Type, Json}, L) ->
                             Header = jsonerl:encode([{<<"index">>, [{<<"_index">>, to_bin(Index)},
                                                                       {<<"_type">>, to_bin(Type)}]}]),
                               L++[Header, <<"\n">>, jsonerl:encode(Json), <<"\n">>]
                     end, [],
                     IndexTypeIdJsonLists),
    erls_resource:post(Params, "/_bulk", [], [], Body, []).

search(Index, Query) ->
    search(#erls_params{}, Index, "", Query, []).

search(Params, Index, Query) when is_record(Params, erls_params) ->
    search(Params, Index, "", Query, []);

%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a query as "key:value" and sends
%% it to the default Elastic Search server on localhost:9100
%%
%% @spec search(Index, Type, Query) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
search(Index, Type, Query) ->
    search(#erls_params{}, Index, Type, Query, []). 

search_limit(Index, Type, Query, Limit) when is_integer(Limit) ->
    search(#erls_params{}, Index, Type, Query, [{"size", lists:flatten(io_lib:format("~B",[Limit]))}]). 
%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a query as "key:value" and sends
%% it to the Elastic Search server specified in Params.
%%
%% @spec search(Params, Index, Type, Query) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
search(Params, Index, Type, Query, Opts) ->
    erls_resource:get(Params, filename:join([erls_utils:join(Index), 
                                             erls_utils:join(Type), "_search"]), 
                      [], [{"q", Query}]++Opts, []).

%% Opts: [{from, 0}, {size, 10}, ....]
q(IndexL, TypeL, OptsPL, QueryType, QueryPL) ->
    q(#erls_params{}, IndexL, TypeL, OptsPL, QueryType, QueryPL).

q(Params, IndexL, TypeL, OptsPL, term, TermPL) ->
    Url = filename:join([erls_utils:join(IndexL), erls_utils:join(TypeL), "_search"]),
    QueryPL = [{<<"query">>, [{term, TermPL}]}],
    BodyPL = lists:append(OptsPL, QueryPL),
    BodyJson = jsonerl:encode(BodyPL),
    erls_resource:post(Params, Url, [], [], BodyJson, []);
q(Params, IndexL, TypeL, OptsPL, match_all, Opt) ->
    Url = filename:join([erls_utils:join(IndexL), erls_utils:join(TypeL), "_search"]),
    QueryPL = [{<<"query">>, [{match_all, Opt}]}],
    BodyPL = lists:append(OptsPL, QueryPL),
    BodyJson = jsonerl:encode(BodyPL),
    erls_resource:post(Params, Url, [], [], BodyJson, []);
q(Params, IndexL, TypeL, OptsPL, query_string, QueryStringPL) ->
    Url = filename:join([erls_utils:join(IndexL), erls_utils:join(TypeL), "_search"]),
    QueryPL = [{<<"query">>, [{query_string, QueryStringPL}]}],
    BodyPL = lists:append(OptsPL, QueryPL),
    io:fwrite("erlastic_search ~p:q/6:BodyPL=~p~n",[?LINE, BodyPL]),
    BodyJson = jsonerl:encode(BodyPL),
    erls_resource:post(Params, Url, [], [], BodyJson, []).

%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a doc id and sends
%% it to the default Elastic Search server on localhost:9100
%%
%% @spec index(Index, Type, Id, Doc) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
get_doc(Index, Type, Id) ->
    get_doc(#erls_params{}, Index, Type, Id).

%%--------------------------------------------------------------------
%% @doc
%% Takes the index and type name and a doc id and sends
%% it to the Elastic Search server specified in Params.
%%
%% @spec index(Params, Index, Type, Id, Doc) -> {ok, Data} | {error, Error}
%% @end
%%--------------------------------------------------------------------
get_doc(Params, Index, Type, Id) ->
    erls_resource:get(Params, filename:join([Index, Type, Id]), [], [], []).

flush_index(Index) ->
    flush_index(#erls_params{}, Index).

flush_index(Params, Index=[H|_T]) when not is_list(H) ->
    flush_index(Params, [Index]);
flush_index(Params, Index) ->
    erls_resource:post(Params, filename:join([erls_utils:comma_separate(Index), "_flush"]), [], [], [], []).

flush_all() ->
    refresh_all(#erls_params{}).

flush_all(Params) ->
    erls_resource:post(Params, "_flush", [], [], [], []).

refresh_index(Index) ->
    refresh_index(#erls_params{}, Index).

refresh_index(Params, Index=[H|_T]) when not is_list(H) ->
    refresh_index(Params, [Index]);
refresh_index(Params, Index) ->
    erls_resource:post(Params, filename:join([erls_utils:comma_separate(Index), "_refresh"]), [], [], [], []).

refresh_all() ->
    refresh_all(#erls_params{}).

refresh_all(Params) ->
    erls_resource:post(Params, "_refresh", [], [], [], []).

delete_doc(Index, Type, Id) ->
    delete_doc(#erls_params{}, Index, Type, Id).

delete_doc(Params, Index, Type, Id) ->
    erls_resource:delete(Params, filename:join([
                                                erls_utils:comma_separate(Index), 
                                                erls_utils:comma_separate(Type), Id]), [], [], []).

delete_doc_by_query(Index, Type, Query) ->
    delete_doc_by_query(#erls_params{}, Index, Type, Query).

delete_doc_by_query(Params, Index, Type, Query) ->
    erls_resource:delete(Params, filename:join(
                                   [erls_utils:comma_separate(Index), 
                                    erls_utils:comma_separate(Type)]), 
                         [], [{"q", Query}], []).

optimize_index(Index) ->
    optimize_index(#erls_params{}, Index).

optimize_index(Params, Index=[H|_T]) when not is_list(H)->
    optimize_index(Params, [Index]);
optimize_index(Params, Index) ->
    erls_resource:post(Params, filename:join([erls_utils:comma_separate(Index), "_optimize"]), [], [], [], []).


build_filter(Field, Value) ->
    Field + ":" + Value.

build_time_range_fileter(Field, From, To) ->
    Field + ":[" + From + " TO " + To + "]".

build_count_facet(Description, Field) ->
    [{Description, [{terms, [{field, Field}]}]}].

build_time_facet(Description, Field, Interval) ->
    [{Description, [{date_histogram, [{field, Field}, {interval, Interval}]}]}].

build_stats_request(Search_string, Facets) -> 
    Size_field = {size, 0},
    Query_field = {<<"query">>, [{query_string, [{<<"query">>, Search_string}]}]},
    Facets_list = lists:foldl(
                    fun({count, Description, Field}, L) ->
                      [build_count_facet(Description, Field)|L];
                       ({Interval, Description, Field}, L) ->
                      [build_time_facet(Description, Field, Interval)|L]
                end, [], Facets),
    Facets_field = {facets, Facets_list},
    [Size_field, Query_field, Facets_field].

get_scroll_id(Json) ->
    proplists:get_value(<<"_scroll_id">>, Json).

get_hits_count(Json) ->
    Hits = proplists:get_value(<<"hits">>, Json),
    proplists:get_value(<<"total">>, Hits).

get_hits_data(Json) ->
    Hits = proplists:get_value(<<"hits">>, Json),
    proplists:get_value(<<"hits">>, Hits).

get_count_stats(Field_description, Json) when is_binary(Field_description) ->
    Facets = proplists:get_value(<<"facets">>, Json),
    Field_facet = proplists:get_value(Field_description, Facets),
    proplists:get_value(<<"terms">>, Field_facet);
get_count_stats(Field_description, Json) ->
    Facets = proplists:get_value(<<"facets">>, Json),
    Field_facet = proplists:get_value(list_to_binary(Field_description), Facets),
    proplists:get_value(<<"terms">>, Field_facet).

get_time_stats(Field_description, Json) when is_binary(Field_description) ->
    Facets = proplists:get_value(<<"facets">>, Json),
    Field_facet = proplists:get_value(Field_description, Facets),
    proplists:get_value(<<"entries">>, Field_facet);
get_time_stats(Field_description, Json) ->
    Facets = proplists:get_value(<<"facets">>, Json),
    Field_facet = proplists:get_value(list_to_binary(Field_description), Facets),
    proplists:get_value(<<"entries">>, Field_facet).

get_next_result(Scroll_id) ->
    erls_resource:get(#erls_params{}, "_search/scroll", [], [{scroll_id, Scroll_id}], []).

get_next_result(Erls_params, Scroll_id) ->
    erls_resource:get(Erls_params, "_search/scroll", [], [{scroll_id, Scroll_id}], []).

get_cluster_health() ->
    erls_resource:get(#erls_params{}, "_cluster/health", [], [], []).

get_cluster_health(Erls_params) ->
    erls_resource:get(Erls_params, "_cluster/health", [], [], []).

get_cluster_state() ->
    erls_resource:get(#erls_params{}, "_cluster/state", [], [], []).

get_cluster_state(Erls_params) ->
    erls_resource:get(Erls_params, "_cluster/state", [], [], []).

get_nodes_info() ->
    erls_resource:get(#erls_params{}, "_cluster/nodes", [], [], []).

get_nodes_info(Erls_params) when is_record(Erls_params, erls_params) ->
    erls_resource:get(Erls_params, "_cluster/nodes", [], [], []);
get_nodes_info(Nodes) ->
    erls_resource:get(#erls_params{}, 
                  filename:join(["_cluster/nodes", 
                                 erls_utils:comma_separate(Nodes)]), 
                  [], [], []).

get_nodes_info(Erls_params, Nodes) ->
    erls_resource:get(Erls_params, 
                  filename:join(["_cluster/nodes", 
                                 erls_utils:comma_separate(Nodes)]), 
                  [], [], []).

get_nodes_stats() ->
    erls_resource:get(#erls_params{}, "_cluster/nodes/stats", [], [], []).

get_nodes_stats(Erls_params) when is_record(Erls_params, erls_params) ->
    erls_resource:get(Erls_params, "_cluster/nodes/stats", [], [], []);
get_nodes_stats(Nodes) ->
    erls_resource:get(#erls_params{}, 
                  filename:join(["_cluster/nodes", 
                                 erls_utils:comma_separate(Nodes), "stats"]), 
                  [], [], []).

get_nodes_stats(Erls_params, Nodes) ->
    erls_resource:get(Erls_params, 
                  filename:join(["_cluster/nodes", 
                                 erls_utils:comma_separate(Nodes), "stats"]), 
                  [], [], []).


