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
-include_lib("emqx/include/emqx_placeholder.hrl").
%% for logging
-include_lib("emqx/include/logger.hrl").

-export([load/3, unload/1]).

%% Client Lifecircle Hooks
-export([
    on_client_connect/3,
    on_client_connack/4,
    on_client_connected/3,
    on_client_disconnected/4,
    on_client_authenticate/3,
    on_client_authorize/5,
    on_client_subscribe/4,
    on_client_unsubscribe/4
]).

%% Session Lifecircle Hooks
-export([
    on_session_created/3,
    on_session_subscribed/4,
    on_session_unsubscribed/4,
    on_session_resumed/3,
    on_session_discarded/3,
    on_session_takenover/3,
    on_session_terminated/4
]).

%% Message Pubsub Hooks
-export([
    on_message_publish/2,
    on_message_delivered/3,
    on_message_acked/3,
    on_message_dropped/4
]).

-import(proplists, [get_value/2, get_value/3]).
-import(emqx_plugin_kafka, [start_kafka/1, start_producer/5, send_msg_to_kafka/6]).

%% Called when the plugin application start
load(ClientConf, ProducerConf, HooKConf) ->
    ClientId = start_kafka(ClientConf),
    HookList = parse_hook(HooKConf),

    NProducers = lists:foldl(
        fun(
            {Hook, Filter, Key, Value, Topic, Seq, TopicProducerConf},
            Acc
        ) ->
            Name = list_to_atom(lists:concat([atom_to_list(Hook), "_", Seq])),

            case start_producer(ClientId, Topic, Name, ProducerConf, TopicProducerConf) of
                {ok, PayloadFormat, Sync, Timeout, Producers} ->
                    load_(
                        Hook,
                        {Filter, Producers, Key, Value, PayloadFormat, Sync, Timeout}
                    ),
                    [Producers | Acc];
                {error, _} ->
                    Acc
            end
        end,
        [],
        HookList
    ),
    logger:info("~s is loaded.~n", [emqx_plugin_kafka]),
    {ok, ClientId, NProducers, HookList}.

load_(Hook, Params) ->
    case Hook of
        'client.connect' ->
            hook(Hook, {?MODULE, on_client_connect, [Params]});
        'client.connack' ->
            hook(Hook, {?MODULE, on_client_connack, [Params]});
        'client.connected' ->
            hook(Hook, {?MODULE, on_client_connected, [Params]});
        'client.disconnected' ->
            hook(Hook, {?MODULE, on_client_disconnected, [Params]});
        'client.authenticate' ->
            hook(Hook, {?MODULE, on_client_authenticate, [Params]});
        'client.authorize' ->
            hook(Hook, {?MODULE, on_client_authorize, [Params]});
        'client.subscribe' ->
            hook(Hook, {?MODULE, on_client_subscribe, [Params]});
        'client.unsubscribe' ->
            hook(Hook, {?MODULE, on_client_unsubscribe, [Params]});
        'session.created' ->
            hook(Hook, {?MODULE, on_session_created, [Params]});
        'session.subscribed' ->
            hook(Hook, {?MODULE, on_session_subscribed, [Params]});
        'session.unsubscribed' ->
            hook(Hook, {?MODULE, on_session_unsubscribed, [Params]});
        'session.resumed' ->
            hook(Hook, {?MODULE, on_session_resumed, [Params]});
        'session.discarded' ->
            hook(Hook, {?MODULE, on_session_discarded, [Params]});
        'session.takenover' ->
            hook(Hook, {?MODULE, on_session_takenover, [Params]});
        'session.terminated' ->
            hook(Hook, {?MODULE, on_session_terminated, [Params]});
        'message.publish' ->
            hook(Hook, {?MODULE, on_message_publish, [Params]});
        'message.delivered' ->
            hook(Hook, {?MODULE, on_message_delivered, [Params]});
        'message.acked' ->
            hook(Hook, {?MODULE, on_message_acked, [Params]});
        'message.dropped' ->
            hook(Hook, {?MODULE, on_message_dropped, [Params]})
    end.

unload(HookList) ->
    lists:foreach(
        fun({Hook, _, _, _, _, _, _}) ->
            unload_(Hook)
        end,
        HookList
    ),
    logger:info("~s is unloaded.~n", [emqx_plugin_kafka]),
    ok.

unload_(Hook) ->
    case Hook of
        'client.connect' ->
            unhook(Hook, {?MODULE, on_client_connect});
        'client.connack' ->
            unhook(Hook, {?MODULE, on_client_connack});
        'client.connected' ->
            unhook(Hook, {?MODULE, on_client_connected});
        'client.disconnected' ->
            unhook(Hook, {?MODULE, on_client_disconnected});
        'client.authenticate' ->
            unhook(Hook, {?MODULE, on_client_authenticate});
        'client.authorize' ->
            unhook(Hook, {?MODULE, on_client_authorize});
        'client.subscribe' ->
            unhook(Hook, {?MODULE, on_client_subscribe});
        'client.unsubscribe' ->
            unhook(Hook, {?MODULE, on_client_unsubscribe});
        'session.created' ->
            unhook(Hook, {?MODULE, on_session_created});
        'session.subscribed' ->
            unhook(Hook, {?MODULE, on_session_subscribed});
        'session.unsubscribed' ->
            unhook(Hook, {?MODULE, on_session_unsubscribed});
        'session.resumed' ->
            unhook(Hook, {?MODULE, on_session_resumed});
        'session.discarded' ->
            unhook(Hook, {?MODULE, on_session_discarded});
        'session.takenover' ->
            unhook(Hook, {?MODULE, on_session_takenover});
        'session.terminated' ->
            unhook(Hook, {?MODULE, on_session_terminated});
        'message.publish' ->
            unhook(Hook, {?MODULE, on_message_publish});
        'message.delivered' ->
            unhook(Hook, {?MODULE, on_message_delivered});
        'message.acked' ->
            unhook(Hook, {?MODULE, on_message_acked});
        'message.dropped' ->
            unhook(Hook, {?MODULE, on_message_dropped})
    end.

hook(HookPoint, MFA) ->
    %% use highest hook priority so this module's callbacks
    %% are evaluated before the default hooks in EMQX
    emqx_hooks:add(HookPoint, MFA, _Property = ?HP_HIGHEST).

unhook(HookPoint, MFA) ->
    emqx_hooks:del(HookPoint, MFA).

on_client_connect(ClientInfo, Props, Rule) ->
    send(ClientInfo, Rule),
    {ok, Props}.

on_client_connack(ClientInfo, _Rc, Props, Rule) ->
    send(ClientInfo, Rule),
    {ok, Props}.

on_client_connected(ClientInfo, _ConnInfo, Rule) ->
    send(ClientInfo, Rule),
    ok.

on_client_disconnected(ClientInfo, _Reason, _ConnInfo, Rule) ->
    send(ClientInfo, Rule),
    ok.

on_client_authenticate(ClientInfo, AuthNResult, Rule) ->
    send(ClientInfo, Rule),
    {ok, AuthNResult}.

on_client_authorize(ClientInfo, _PubSub, _Topic, AuthNResult, Rule) ->
    send(ClientInfo, Rule),
    {ok, AuthNResult}.

on_client_subscribe(ClientInfo, _Props, TopicFilters, Rule) ->
    send(ClientInfo, Rule),
    {ok, TopicFilters}.

on_client_unsubscribe(ClientInfo, _Props, TopicFilters, Rule) ->
    send(ClientInfo, Rule),
    {ok, TopicFilters}.

on_session_created(ClientInfo, _SessInfo, Rule) ->
    send(ClientInfo, Rule),
    ok.

on_session_subscribed(ClientInfo, Topic, Opts, Rule) ->
    send(ClientInfo, Topic, maps:get(qos, Opts, 0), Rule),
    ok.

on_session_unsubscribed(ClientInfo, Topic, Opts, Rule) ->
    send(ClientInfo, Topic, maps:get(qos, Opts, 0), Rule),
    ok.

on_session_resumed(ClientInfo, _SessInfo, Rule) ->
    send(ClientInfo, Rule),
    ok.

on_session_discarded(ClientInfo, _SessInfo, Rule) ->
    send(ClientInfo, Rule),
    ok.

on_session_takenover(ClientInfo, _SessInfo, Rule) ->
    send(ClientInfo, Rule),
    ok.

on_session_terminated(ClientInfo, _Reason, _SessInfo, Rule) ->
    send(ClientInfo, Rule),
    ok.

on_message_publish(Msg = #message{topic = <<"$SYS/", _/binary>>}, _Rule) ->
    {ok, Msg};
on_message_publish(Msg, Rule) ->
    send(Msg, Rule),
    {ok, Msg}.

on_message_delivered(_ClientInfo, Msg, Rule) ->
    send(Msg, Rule),
    {ok, Msg}.

on_message_acked(_ClientInfo, Msg, Rule) ->
    send(Msg, Rule),
    ok.

on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason, _Rule) ->
    ok;
on_message_dropped(Msg, _By, _Reson, Rule) ->
    send(Msg, Rule),
    ok.

parse_hook(Hooks) -> parse_hook(Hooks, [], 0).

parse_hook([], Acc, _Seq) ->
    Acc;
parse_hook([Item | Hooks], Acc, Seq) ->
    Hook = maps:get(<<"hook">>, Item),
    Topic = maps:get(<<"topic">>, Item),
    Filter = maps:get(<<"filter">>, Item, "#"),
    Key = format_value_pattern(maps:get(<<"key">>, Item, undefined)),
    Value = format_value_pattern(maps:get(<<"value">>, Item)),
    NewSeq = Seq + 1,
    ProducerConf = maps:get(<<"producer">>, Item, #{}),

    parse_hook(
        Hooks,
        [
            {
                erlang:list_to_atom(Hook),
                erlang:list_to_binary(Filter),
                Key,
                Value,
                erlang:list_to_binary(Topic),
                NewSeq,
                ProducerConf
            }
            | Acc
        ],
        NewSeq
    ).

format_value_pattern(undefined) ->
    [];
format_value_pattern(Value) ->
    emqx_placeholder:preproc_tmpl(Value).

send(
    #{clientid := ClientId, username := Username},
    {_, Producers, Key, Value, _, Sync, Timeout}
) ->
    send_msg_to_kafka(
        Producers,
        {
            node(),
            ClientId,
            undefined,
            {undefined, undefined},
            Username,
            undefined,
            erlang:system_time(millisecond)
        },
        Key,
        Value,
        Sync,
        Timeout
    );
send(
    #message{
        from = From,
        topic = Topic,
        payload = Payload,
        headers = Headers,
        qos = Qos,
        timestamp = Ts
    },
    {Filter, Producers, Key, Value, PayloadFormat, Sync, Timeout}
) ->
    case emqx_topic:match(Topic, Filter) of
        true ->
            send_msg_to_kafka(
                Producers,
                {
                    node(),
                    From,
                    Topic,
                    {Payload, PayloadFormat},
                    maps:get(username, Headers, <<>>),
                    Qos,
                    Ts
                },
                Key,
                Value,
                Sync,
                Timeout
            );
        false ->
            ok
    end.

send(ClientInfo, Topic, Qos, {Filter, Producers, Key, Value, _, Sync, Timeout}) ->
    case emqx_topic:match(Topic, Filter) of
        true ->
            ClientId = maps:get(clientid, ClientInfo, undefined),
            Username = maps:get(username, ClientInfo, undefined),
            send_msg_to_kafka(
                Producers,
                {
                    node(),
                    ClientId,
                    Topic,
                    {undefined, undefined},
                    Username,
                    Qos,
                    erlang:system_time(millisecond)
                },
                Key,
                Value,
                Sync,
                Timeout
            );
        false ->
            ok
    end.
