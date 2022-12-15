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
    start_kafka/1, send_msg_to_kafka/4, start_producer/5
]).

-spec start_kafka(map()) -> binary().
start_kafka(ClientConf) ->
    ClientId = erlang:list_to_binary(maps:get(<<"client_id">>, ClientConf, "emqx_kafka")),
    Address = maps:get(<<"servers">>, ClientConf, ["localhost:9092"]),
    KafkaEndpoints = lists:map(
        fun(S) ->
            Arr = string:split(S, ":"),
            Host = lists:nth(1, Arr),
            Port = list_to_integer(lists:nth(2, Arr)),
            {Host, Port}
        end,
        Address
    ),

    Conf = #{
        connection_strategy => maps:get(<<"(connection_strategy">>, ClientConf, per_partition),
        min_metadata_refresh_interval => maps:get(
            <<"min_metadata_refresh_interval">>, ClientConf, 5000
        ),
        query_api_versions => maps:get(<<"query_api_versions">>, ClientConf, true),
        request_timeout => maps:get(<<"request_timeout">>, ClientConf, 240000)
    },

    logger:debug("connect kafka ~p ~p ~p ", [ClientId, KafkaEndpoints, Conf]),

    {ok, _} = application:ensure_all_started(wolff),
    {ok, _ClientPid} =
        wolff:ensure_supervised_client(
            ClientId,
            KafkaEndpoints,
            Conf
        ),
    ClientId.

-spec start_producer(binary(), binary(), atom(), map(), map()) ->
    {ok, atom(), integer(), wolff:producers()} | {error, any()}.
start_producer(ClientId, Topic, Name, ProducerConf, TopicProducerConf) ->
    Cfg = maps:merge(ProducerConf, TopicProducerConf),

    MaybeReplayqDir = maps:get(<<"replayq_dir">>, Cfg, false),
    ReplayqDir =
        case MaybeReplayqDir of
            false -> undefined;
            _ -> filename:join([MaybeReplayqDir, node()])
        end,

    Sync = maps:get(<<"produce">>, Cfg, sync),
    Timeout = maps:get(<<"produce_sync_timeout">>, Cfg, 3000),
    PayloadFormat = maps:get(<<"encode_payload_type">>, Cfg, plain),

    Conf = #{
        partitioner => maps:get(<<"partitioner">>, Cfg, random),
        compression => maps:get(<<"compression">>, Cfg, no_compression),
        replayq_dir => ReplayqDir,
        name => Name
    },

    case wolff:ensure_supervised_producers(ClientId, Topic, Conf) of
        {ok, Producers} ->
            logger:debug("connect producer ~p ~p ~p ", [ClientId, Topic, ProducerConf]),
            {ok, PayloadFormat, Sync, Timeout, Producers};
        {error, Error} ->
            logger:error("Start topic:~p producers fail, error:~p", [Topic, Error]),
            wolff:stop_and_delete_supervised_producers(ClientId, Topic, Name),
            {error, Error}
    end.

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
    logger:debug("Produce key: ~p, payload: ~p ,sync: ~p ,timeout: ~p ,producers: ~p ", [
        Key, JsonMsg, Sync, Timeout, Producers
    ]),
    case Sync of
        sync ->
            wolff:send_sync(
                Producers, [#{key => Key, value => JsonMsg}], Timeout
            );
        async ->
            wolff:send(
                Producers, [#{key => Key, value => JsonMsg}], fun(_Partition, _BaseOffset) -> ok end
            )
    end.
