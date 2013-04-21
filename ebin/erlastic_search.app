{application,erlastic_search,
             [{description,"An Erlang app for communicating with Elastic Search's rest interface."},
              {vsn,"0.2.0"},
              {modules,[boss_db_adapter_es,date_format,date_util,
                        erlastic_search,erls_query_constructor,erls_resource,
                        erls_utils,es_api,exprecs,jsonerl]},
              {registered,[]},
              {applications,[kernel,stdlib,sasl,public_key,ssl,crypto,lhttpc]},
              {start_phases,[]}]}.
