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
    start_kafka/1, send_msg_to_kafka/6, start_producer/5
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

    Conf = maps:fold(
        fun(Key, Value, Acc) ->
            case Key of
                <<"connection_strategy">> ->
                    [{connection_strategy, Value} | Acc];
                <<"min_metadata_refresh_interval">> ->
                    [{min_metadata_refresh_interval, Value} | Acc];
                <<"query_api_versions">> ->
                    [{query_api_versions, Value} | Acc];
                <<"request_timeout">> ->
                    [{request_timeout, Value} | Acc];
                <<"sasl">> ->
                    [
                        {sasl, {
                            maps:get(<<"mechanism">>, Value, plain),
                            maps:get(<<"username">>, Value, undefined),
                            maps:get(<<"password">>, Value, undefined)
                        }}
                        | Acc
                    ];
                <<"ssl">> ->
                    [
                        {ssl,
                            maps:fold(
                                fun(SslPropKey, SslPropValue, SslAcc) ->
                                    case SslPropKey of
                                        <<"verify">> ->
                                            [{verify, SslPropValue} | SslAcc];
                                        <<"ca_cert_file">> ->
                                            [{cacertfile, SslPropValue} | SslAcc];
                                        <<"depth">> ->
                                            [{depth, SslPropValue} | SslAcc];
                                        <<"customize_hostname_check">> ->
                                            [
                                                {customize_hostname_check, [
                                                    {match_fun,
                                                        public_key:pkix_verify_hostname_match_fun(
                                                            https
                                                        )}
                                                ]}
                                                | SslAcc
                                            ]
                                    end
                                end,
                                [],
                                Value
                            )}
                        | Acc
                    ];
                _ ->
                    Acc
            end
        end,
        [],
        ClientConf
    ),

    logger:debug("connect kafka ~p ~p ~p ", [ClientId, KafkaEndpoints, Conf]),

    {ok, _} = application:ensure_all_started(wolff),
    {ok, _ClientPid} =
        wolff:ensure_supervised_client(
            ClientId,
            KafkaEndpoints,
            maps:from_list(Conf)
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

send_msg_to_kafka(Producers, Msg, KeyPattern, ValuePattern, Sync, Timeout) ->
    Value = fill_value_pattern(ValuePattern, Msg),
    Key = fill_value_pattern(KeyPattern, Msg),

    try
        produce(Producers, data_format(Key), data_format(Value), Sync, Timeout)
    catch
        Error:Reason:Stask ->
            logger:error("Call produce error: ~p, ~p", [Error, {Reason, Stask}])
    end.

produce(Producers, Key, Value, Sync, Timeout) when is_list(Value) ->
    produce(Producers, Key, iolist_to_binary(Value), Sync, Timeout);
produce(Producers, Key, Value, Sync, Timeout) ->
    logger:debug("Produce key: ~p, payload: ~p ,sync: ~p ,timeout: ~p ,producers: ~p ", [
        Key, Value, Sync, Timeout, Producers
    ]),
    case Sync of
        sync ->
            wolff:send_sync(
                Producers, [#{key => Key, value => Value}], Timeout
            );
        async ->
            wolff:send(
                Producers, [#{key => Key, value => Value}], fun(_Partition, _BaseOffset) -> ok end
            )
    end.

fill_value_pattern(undefined, _) ->
    <<>>;
fill_value_pattern(ValuePattern, {Node, From, Topic, {Payload, PayloadFormat}, Username, Qos, Ts}) ->
    iolist_to_binary(
        string:join(
            lists:map(
                fun
                    ({str, Str}) ->
                        binary_to_list(Str);
                    ({var, {var, Key}}) ->
                        case Key of
                            <<"clientid">> -> binary_to_list(From);
                            <<"from">> -> binary_to_list(From);
                            <<"username">> -> binary_to_list(Username);
                            <<"topic">> -> binary_to_list(Topic);
                            <<"payload">> -> binary_to_list(payload_format(Payload, PayloadFormat));
                            <<"qos">> -> integer_to_list(Qos);
                            <<"node">> -> binary_to_list(a2b(Node));
                            <<"ts">> -> integer_to_list(Ts)
                        end
                end,
                ValuePattern
            ),
            ""
        )
    ).

payload_format(Payload, PayloadFormat) ->
    case PayloadFormat of
        base64 -> base64:encode(Payload);
        _ -> Payload
    end.

data_format(Data) when is_binary(Data) ->
    Data;
data_format(Data) ->
    emqx_json:encode(Data).

a2b(A) when is_atom(A) ->
    erlang:atom_to_binary(A, utf8);
a2b(A) ->
    A.
