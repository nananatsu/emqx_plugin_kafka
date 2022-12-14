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

-module(emqx_plugin_kafka_hook).

%% for #message{} record
%% no need for this include if we call emqx_message:to_map/1 to convert it to a map
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").

%% for logging
-include_lib("emqx/include/logger.hrl").

-export([load/1, unload/1]).

-export([
    on_client_connected/3,
    on_client_disconnected/4,
    on_session_subscribed/4,
    on_session_unsubscribed/4,
    on_message_publish/2,
    on_message_delivered/3,
    on_message_acked/3
]).

-import(proplists, [get_value/2, get_value/3]).
-import(emqx_plugin_kafka, [start_kafka/1, send_msg_to_kafka/4]).

%% Called when the plugin application start
load(Conf) ->
    ClientId = start_kafka(Conf),

    HookList = parse_hook(get_value(filter, Conf, [])),
    MaybeReplayqDir = get_value(replayq_dir, Conf, false),
    ReplayqDir =
        case MaybeReplayqDir of
            false ->
                undefined;
            _ ->
                filename:join([MaybeReplayqDir, node()])
        end,
    ProducerConf = get_value(producer, Conf, []),
    Sync = get_value(produce, Conf, sync),
    Timeout = get_value(produce_sync_timeout, Conf, 3000),

    NProducers = lists:foldl(
        fun(
            {Hook, Filter, Key, Value, Topic, Strategy, Seq, PayloadFormat},
            Acc
        ) ->
            Name = list_to_atom(lists:concat([atom_to_list(Hook), "_", Seq])),
            ProducerConfig =
                ProducerConf ++
                    [
                        {partitioner, Strategy},
                        {replayq_dir, ReplayqDir},
                        {name, Name}
                    ],
            case
                wolff:ensure_supervised_producers(ClientId, Topic, maps:from_list(ProducerConfig))
            of
                {ok, Producers} ->
                    load_(
                        Hook,
                        {Filter, Producers, Key, Value, PayloadFormat, Sync, Timeout}
                    ),
                    [Producers | Acc];
                {error, Error} ->
                    logger:error("Start topic:~p producers fail, error:~p", [Topic, Error]),
                    wolff:stop_and_delete_supervised_producers(ClientId, Topic, Name),
                    Acc
            end
        end,
        [],
        HookList
    ),
    logger:info("~s is loaded.~n", [emqx_plugin_kafka]),
    {ok, ClientId, NProducers}.

load_(Hook, Params) ->
    case Hook of
        'client.connected' ->
            emqx:hook(
                Hook,
                fun emqx_plugin_kafka:on_client_connected/3,
                [Params]
            );
        'client.disconnected' ->
            emqx:hook(
                Hook,
                fun emqx_plugin_kafka:on_client_disconnected/4,
                [Params]
            );
        'session.subscribed' ->
            emqx:hook(
                Hook,
                fun emqx_plugin_kafka:on_session_subscribed/4,
                [Params]
            );
        'session.unsubscribed' ->
            emqx:hook(
                Hook,
                fun emqx_plugin_kafka:on_session_unsubscribed/4,
                [Params]
            );
        'message.publish' ->
            emqx:hook(
                Hook,
                fun emqx_plugin_kafka:on_message_publish/2,
                [Params]
            );
        'message.acked' ->
            emqx:hook(
                Hook,
                fun emqx_plugin_kafka:on_message_acked/3,
                [Params]
            );
        'message.delivered' ->
            emqx:hook(
                Hook,
                fun emqx_plugin_kafka:on_message_delivered/3,
                [Params]
            )
    end.

unload(Conf) ->
    HookList = parse_hook(get_value(filter, Conf, [])),
    lists:foreach(
        fun({Hook, _, _, _, _, _, _, _, _}) ->
            unload_(Hook)
        end,
        HookList
    ),
    logger:info("~s is unloaded.~n", [emqx_plugin_kafka]),
    ok.

unload_(Hook) ->
    case Hook of
        'client.connected' ->
            emqx:unhook(
                Hook,
                fun emqx_plugin_kafka:on_client_connected/3
            );
        'client.disconnected' ->
            emqx:unhook(
                Hook,
                fun emqx_plugin_kafka:on_client_disconnected/4
            );
        'session.subscribed' ->
            emqx:unhook(
                Hook,
                fun emqx_plugin_kafka:on_session_subscribed/4
            );
        'session.unsubscribed' ->
            emqx:unhook(
                Hook,
                fun emqx_plugin_kafka:on_session_unsubscribed/4
            );
        'message.publish' ->
            emqx:unhook(
                Hook,
                fun emqx_plugin_kafka:on_message_publish/2
            );
        'message.acked' ->
            emqx:unhook(
                Hook,
                fun emqx_plugin_kafka:on_message_acked/3
            );
        'message.delivered' ->
            emqx:unhook(
                Hook,
                fun emqx_plugin_kafka:on_message_delivered/3
            )
    end.

on_client_connected(
    ClientInfo,
    _ConnInfo,
    {_, Producers, Key, _, Sync, Timeout}
) ->
    logger:info("client connect: ~n ~p ~n ~p~n", [ClientInfo, _ConnInfo]),
    ClientId = maps:get(clientid, ClientInfo, undefined),
    Username = maps:get(username, ClientInfo, undefined),
    Data = [
        {clientid, ClientId},
        {username, Username},
        {node, a2b(node())},
        {ts, erlang:system_time(millisecond)}
    ],
    send_msg_to_kafka(
        Producers,
        {feed_key(Key, {ClientId, Username}), data_format(Data)},
        Sync,
        Timeout
    ),
    ok.

on_client_disconnected(
    ClientInfo,
    {shutdown, Reason},
    ConnInfo,
    Rule
) when
    is_atom(Reason); is_integer(Reason)
->
    on_client_disconnected(
        ClientInfo,
        Reason,
        ConnInfo,
        Rule
    );
on_client_disconnected(
    ClientInfo,
    Reason,
    _ConnInfo,
    {_, Producers, Key, _, _, Sync, Timeout}
) when
    is_atom(Reason); is_integer(Reason)
->
    logger:info("client disconnected: ~n ~p ~n ~p~n", [ClientInfo, Reason]),
    ClientId = maps:get(clientid, ClientInfo, undefined),
    Username = maps:get(username, ClientInfo, undefined),
    Data = [
        {clientid, ClientId},
        {username, Username},
        {node, a2b(node())},
        {reason, a2b(Reason)},
        {ts, erlang:system_time(millisecond)}
    ],
    send_msg_to_kafka(
        Producers,
        {feed_key(Key, {ClientId, Username}), data_format(Data)},
        Sync,
        Timeout
    ),
    ok;
on_client_disconnected(
    _ClientInfo,
    Reason,
    _ConnInfo,
    _Envs
) ->
    logger:error(
        "Client disconnected reason:~p not encode "
        "json",
        [Reason]
    ),
    ok.

on_session_subscribed(
    ClientInfo,
    Topic,
    Opts,
    {Filter, Producers, Key, _, _, Sync, Timeout}
) ->
    case emqx_topic:match(Topic, Filter) of
        true ->
            ClientId = maps:get(clientid, ClientInfo, undefined),
            Username = maps:get(username, ClientInfo, undefined),
            Data = format_sub_json(ClientId, Topic, Opts),
            send_msg_to_kafka(
                Producers,
                {feed_key(Key, {ClientId, Username, Topic}), data_format(Data)},
                Sync,
                Timeout
            );
        false ->
            ok
    end,
    ok.

on_session_unsubscribed(
    ClientInfo,
    Topic,
    Opts,
    {Filter, Producers, Key, _, _, Sync, Timeout}
) ->
    case emqx_topic:match(Topic, Filter) of
        true ->
            ClientId = maps:get(clientid, ClientInfo, undefined),
            Username = maps:get(username, ClientInfo, undefined),
            Data = format_sub_json(ClientId, Topic, Opts),
            send_msg_to_kafka(
                Producers,
                {feed_key(Key, {ClientId, Username, Topic}), data_format(Data)},
                Sync,
                Timeout
            );
        false ->
            ok
    end,
    ok.

on_message_publish(
    Msg = #message{
        topic = Topic,
        from = From,
        headers = Headers
    },
    {Filter, Producers, Key, Value, PayloadFormat, Sync, Timeout}
) ->
    case emqx_topic:match(Topic, Filter) of
        true ->
            Data = format_pub_msg(Msg, Value, PayloadFormat),

            Username = maps:get(username, Headers, <<>>),
            send_msg_to_kafka(
                Producers,
                {feed_key(Key, {From, Username, Topic}), data_format(Data)},
                Sync,
                Timeout
            );
        false ->
            ok
    end,
    {ok, Msg}.

on_message_acked(
    ClientInfo,
    Msg = #message{topic = Topic},
    {Filter, Producers, Key, _, PayloadFormat, Sync, Timeout}
) ->
    case emqx_topic:match(Topic, Filter) of
        true ->
            ClientId = maps:get(clientid, ClientInfo, undefined),
            Username = maps:get(username, ClientInfo, undefined),
            Data = format_revc_msg(
                ClientId,
                Username,
                Msg,
                PayloadFormat
            ),
            send_msg_to_kafka(
                Producers,
                {feed_key(Key, {ClientId, Username, Topic}), data_format(Data)},
                Sync,
                Timeout
            );
        false ->
            ok
    end,
    {ok, Msg}.

on_message_delivered(
    ClientInfo,
    Msg = #message{topic = Topic},
    {Filter, Producers, Key, _, PayloadFormat, Sync, Timeout}
) ->
    case emqx_topic:match(Topic, Filter) of
        true ->
            ClientId = maps:get(clientid, ClientInfo, undefined),
            Username = maps:get(username, ClientInfo, undefined),
            Data = format_revc_msg(
                ClientId,
                Username,
                Msg,
                PayloadFormat
            ),
            send_msg_to_kafka(
                Producers,
                {feed_key(Key, {ClientId, Username, Topic}), data_format(Data)},
                Sync,
                Timeout
            );
        false ->
            ok
    end,
    {ok, Msg}.

parse_hook(Hooks) -> parse_hook(Hooks, [], 0).

parse_hook([], Acc, _Seq) ->
    Acc;
parse_hook([Item | Hooks], Acc, Seq) ->
    Hook = maps:get(<<"hook">>, Item),
    Topic = maps:get(<<"topic">>, Item),
    Filter = maps:get(<<"filter">>, Item),
    Key = maps:get(<<"key">>, Item),
    Value = format_value_pattern(maps:get(<<"value">>, Item)),
    Strategy = maps:get(<<"strategy">>, Item, "random"),
    PayloadFormat = maps:get(<<"encode_payload_type">>, Item, "base64"),
    NewSeq = Seq + 1,

    parse_hook(
        Hooks,
        [
            {
                erlang:list_to_atom(Hook),
                Filter,
                Key,
                Value,
                Topic,
                Strategy,
                NewSeq,
                PayloadFormat
            }
            | Acc
        ],
        NewSeq
    ).

format_sub_json(ClientId, Topic, Opts) ->
    Qos = maps:get(qos, Opts, 0),
    [
        {clientid, ClientId},
        {topic, Topic},
        {qos, Qos},
        {node, a2b(node())},
        {ts, erlang:system_time(millisecond)}
    ].

payload_format(Payload, PayloadFormat) ->
    case PayloadFormat of
        <<"base64">> -> base64:encode(Payload);
        _ -> Payload
    end.

format_pub_msg(Msg, undefined, PayloadFormat) ->
    #message{
        from = From,
        topic = Topic,
        payload = Payload,
        headers = Headers,
        qos = Qos,
        timestamp = Ts
    } =
        Msg,
    Username = maps:get(username, Headers, <<>>),
    [
        {clientid, From},
        {username, Username},
        {topic, Topic},
        {payload, payload_format(Payload, PayloadFormat)},
        {qos, Qos},
        {node, a2b(node())},
        {ts, Ts}
    ];
format_pub_msg(Msg, Value, PayloadFormat) ->
    #message{
        from = From,
        topic = Topic,
        payload = Payload,
        headers = Headers,
        qos = Qos,
        timestamp = Ts
    } =
        Msg,
    iolist_to_binary(
        string:join(
            lists:map(
                fun
                    ({str, Str}) ->
                        binary_to_list(Str);
                    ({var, {var, Key}}) ->
                        case Key of
                            <<"from">> -> binary_to_list(From);
                            <<"username">> -> binary_to_list(maps:get(username, Headers, <<>>));
                            <<"topic">> -> binary_to_list(Topic);
                            <<"payload">> -> binary_to_list(payload_format(Payload, PayloadFormat));
                            <<"qos">> -> integer_to_list(Qos);
                            <<"node">> -> binary_to_list(a2b(node()));
                            <<"ts">> -> integer_to_list(Ts)
                        end
                end,
                Value
            ),
            ""
        )
    ).

format_revc_msg(
    ClientId,
    Username,
    Msg,
    PayloadFormat
) ->
    #message{
        from = From,
        topic = Topic,
        payload = Payload,
        qos = Qos,
        timestamp = Ts
    } =
        Msg,
    [
        {clientid, ClientId},
        {username, Username},
        {from, From},
        {topic, Topic},
        {payload, payload_format(Payload, PayloadFormat)},
        {qos, Qos},
        {node, a2b(node())},
        {ts, Ts}
    ].

data_format(Data) when is_binary(Data) ->
    Data;
data_format(Data) ->
    emqx_json:encode(Data).

a2b(A) when is_atom(A) ->
    erlang:atom_to_binary(A, utf8);
a2b(A) ->
    A.

format_value_pattern(undefined) ->
    undefined;
format_value_pattern(Value) ->
    emqx_rule_utils:preproc_tmpl(Value).

feed_key(undefined, _) ->
    <<>>;
feed_key(<<"${clientid}">>, {ClientId, _Username}) ->
    ClientId;
feed_key(<<"${username}">>, {_ClientId, Username}) ->
    Username;
feed_key(
    <<"${clientid}">>,
    {ClientId, _Username, _Topic}
) ->
    ClientId;
feed_key(
    <<"${username}">>,
    {_ClientId, Username, _Topic}
) ->
    Username;
feed_key(
    <<"${topic}">>,
    {_ClientId, _Username, Topic}
) ->
    Topic;
feed_key(Key, {_ClientId, _Username, Topic}) ->
    case
        re:run(
            Key,
            <<"{([^}]+)}">>,
            [{capture, all, binary}, global]
        )
    of
        nomatch ->
            <<>>;
        {match, Match} ->
            TopicWords = emqx_topic:words(Topic),
            lists:foldl(
                fun([_, Index], Acc) ->
                    Word = lists:nth(
                        binary_to_integer(Index),
                        TopicWords
                    ),
                    <<Acc/binary, Word/binary>>
                end,
                <<>>,
                Match
            )
    end.
