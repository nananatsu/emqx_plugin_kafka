-module(emqx_plugin_kafka_schema).

-export([roots/0, fields/1]).

-include_lib("hocon/include/hoconsc.hrl").

roots() -> [kafka].

fields(kafka) ->
    [
        {servers, ?HOCON(?ARRAY(typerefl:string()), #{mapping => "kafka.servers"})},
        {client_id, ?HOCON(typerefl:string(), #{mapping => "kafka.client_id"})},
        {query_api_versions, ?HOCON(typerefl:boolean(), #{mapping => "kafka.query_api_versions"})},
        {connection_strategy, ?HOCON(typerefl:string(), #{mapping => "kafka.connection_strategy"})},
        {min_metadata_refresh_interval,
            ?HOCON(typerefl:string(), #{mapping => "kafka.min_metadata_refresh_interval"})},
        {produce, ?HOCON(typerefl:string(), #{mapping => "kafka.produce"})},
        {produce_sync_timeout,
            ?HOCON(typerefl:string(), #{mapping => "kafka.produce_sync_timeout"})},
        {encode_payload_type, ?HOCON(typerefl:string(), #{mapping => "kafka.encode_payload_type"})},
        {sock_sndbuf, ?HOCON(typerefl:string(), #{mapping => "kafka.sock_sndbuf"})},
        % {producer, ?HOCON(?MAP(name, ?UNION([?R_REF(producer)])), #{mapping => "kafka.producer"})},
        {filter, ?HOCON(?ARRAY(?UNION([?R_REF(filter)])), #{mapping => "kafka.filter"})}
    ];
% fields(producer) ->
%     [
%         {server, typerefl:boolean()},
%         {query_api_versions, typerefl:boolean()},
%         {connection_strategy, typerefl:string()},
%         {min_metadata_refresh_interval, typerefl:string()},
%         {produce, typerefl:string()},
%         {produce_sync_timeout, typerefl:string()},
%         {encode_payload_type, typerefl:string()},
%         {sock_sndbuf, typerefl:string()}
%     ];
fields(filter) ->
    [
        {hook, typerefl:string()},
        {filter, typerefl:string()},
        {topic, typerefl:string()},
        {value, typerefl:string()}
    ].
