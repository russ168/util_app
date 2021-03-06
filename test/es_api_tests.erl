-module(es_api_tests).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

start_app() ->
    code:add_path("../../lhttpc/ebin"),
    code:add_path("../ebin"),
    application:start(public_key),
    ok = application:start(ssl),
    ok = application:start(lhttpc),
    ok.
    
stop_app(_) ->
    ok = application:stop(lhttpc),
    ok = application:stop(ssl),
    ok.

manager_test_() ->
    {inorder,
        {setup, fun start_app/0, fun stop_app/1, [
                {"get cluster health", fun() ->
                        Res = es_api:get_cluster_health(), ?debugVal(Res), ?assertMatch({ok, _}, Res) 
                end},
                {"get cluster state", fun() ->
                        Res = es_api:get_cluster_state(), ?debugVal(Res), ?assertMatch({ok, _}, Res) 
                end},
                {"get nodes info", fun() ->
                        Res = es_api:get_nodes_info(), ?debugVal(Res), ?assertMatch({ok, _}, Res) 
                end},
                {"get nodes stats", fun() ->
                        Res = es_api:get_nodes_stats(), ?debugVal(Res), ?assertMatch({ok, _}, Res) 
                end}

            ]}
    }.

%% test_one() ->
%%     Res = es_api:get_cluster_health(),
%%     ?debugVal(Res),
%%     ?assertMatch({xx, _}, Res),
%%     ok.
