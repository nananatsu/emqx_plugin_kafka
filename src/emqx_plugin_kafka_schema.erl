-module(emqx_plugin_kafka_schema).

-export([roots/0, fields/1]).

-include_lib("hocon/include/hoconsc.hrl").

roots() -> [kafka].

fields(kafka) ->
    [
        {client, ?HOCON(?R_REF(client), #{mapping => "kafka.client"})},
        {producer, ?HOCON(?R_REF(producer), #{mapping => "kafka.producer"})},
        {hook, ?HOCON(?ARRAY(?UNION([?R_REF(hook)])), #{mapping => "kafka.hook"})}
    ];
fields(client) ->
    [
        {client_id, typerefl:string()},
        {servers, ?ARRAY(typerefl:string())},
        {connection_strategy, typerefl:atom()},
        {min_metadata_refresh_interval, typerefl:integer()},
        {query_api_versions, typerefl:boolean()},
        {request_timeout, typerefl:integer()},
        {sasl, ?R_REF(sasl)},
        {ssl, ?R_REF(ssl)}
    ];
fields(sasl) ->
    [
        {mechanism, typerefl:atom()},
        {username, typerefl:string()},
        {password, typerefl:string()}
    ];
fields(ssl) ->
    [
        {verify, typerefl:atom()},
        {ca_cert_file, typerefl:string()},
        {depth, typerefl:integer()},
        {customize_hostname_check, typerefl:boolean()}
    ];
fields(producer) ->
    [
        {produce, typerefl:atom()},
        {produce_sync_timeout, typerefl:integer()},
        {encode_payload_type, typerefl:atom()},
        {replayq_dir, typerefl:atom()},
        {compression, typerefl:atom()},
        {partitioner, typerefl:atom()}
    ];
fields(hook) ->
    [
        {hook, typerefl:string()},
        {filter, typerefl:string()},
        {topic, typerefl:string()},
        {key, typerefl:string()},
        {value, typerefl:string()},
        {producer, ?R_REF(producer)}
    ].
