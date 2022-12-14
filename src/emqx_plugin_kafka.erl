%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugin_kafka).

-export([
    start_kafka/1, send_msg_to_kafka/4
]).

-import(proplists, [get_value/2, get_value/3]).

start_kafka(Conf) ->
    ClientId = erlang:list_to_binary(get_value(client_id, Conf, "emqx_kafka")),
    Address = get_value(servers, Conf, ["localhost:9092"]),
    KafkaEndpoints = lists:map(
        fun(S) ->
            Arr = string:split(S, ":"),
            Host = lists:nth(1, Arr),
            Port = list_to_integer(lists:nth(2, Arr)),
            {Host, Port}
        end,
        Address
    ),

    ClientConf = #{
        extra_sock_opts => get_value(sock_opts, Conf, []),
        connection_strategy => get_value(connection_strategy, Conf, per_partition),
        min_metadata_refresh_interval => get_value(min_metadata_refresh_interval, Conf, 5000),
        query_api_versions => get_value(query_api_versions, Conf, true)
    },

    {ok, _} = application:ensure_all_started(wolff),
    {ok, _ClientPid} =
        wolff:ensure_supervised_client(
            ClientId,
            KafkaEndpoints,
            ClientConf
        ),
    ClientId.

send_msg_to_kafka(Producers, {Key, JsonMsg}, Sync, Timeout) ->
    try
        produce(Producers, Key, JsonMsg, Sync, Timeout)
    catch
        Error:Reason:Stask ->
            logger:error("Call produce error: ~p, ~p", [Error, {Reason, Stask}])
    end.

produce(Producers, Key, JsonMsg, Sync, Timeout) when is_list(JsonMsg) ->
    produce(Producers, Key, iolist_to_binary(JsonMsg), Sync, Timeout);
produce(Producers, Key, JsonMsg, Sync, Timeout) ->
    logger:debug("Produce key: ~p, payload: ~p ", [Key, JsonMsg]),
    case Sync of
        sync ->
            wolff:send_sync(Producers, [#{key => Key, value => JsonMsg}], Timeout);
        async ->
            wolff:send(
                Producers, [#{key => Key, value => JsonMsg}], fun(_Partition, _BaseOffset) -> ok end
            )
    end.
