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

%% 获取或创建producer，使用缓存避免重复创建相同topic的producer
get_or_create_producer(ClientId, Topic, Name, ProducerConf, TopicProducerConf, ProducerCache) ->
    %% 生成缓存键，基于topic和配置的hash
    CacheKey = {Topic, erlang:phash2({ProducerConf, TopicProducerConf})},

    case ets:lookup(ProducerCache, CacheKey) of
        [{CacheKey, {PayloadFormat, Sync, Timeout, Producers}}] ->
            %% 从缓存中返回已存在的producer
            logger:debug("Reusing cached producer for topic: ~p", [Topic]),
            {ok, PayloadFormat, Sync, Timeout, Producers};
        [] ->
            %% 缓存中不存在，创建新的producer
            case start_producer(ClientId, Topic, Name, ProducerConf, TopicProducerConf) of
                {ok, PayloadFormat, Sync, Timeout, Producers} ->
                    %% 将新创建的producer存入缓存
                    ets:insert(
                        ProducerCache, {CacheKey, {PayloadFormat, Sync, Timeout, Producers}}
                    ),
                    logger:debug("Created and cached new producer for topic: ~p", [Topic]),
                    {ok, PayloadFormat, Sync, Timeout, Producers};
                {error, Error} ->
                    {error, Error}
            end
    end.

%% Called when the plugin application start
load(ClientConf, ProducerConf, HooKConf) ->
    ClientId = start_kafka(ClientConf),
    GroupedHooks = parse_hook(HooKConf),

    %% 创建producer缓存表
    ProducerCache = ets:new(kafka_producer_cache, [set, private]),

    {NProducers, ProcessedHooks} = lists:foldl(
        fun({Hook, Rules}, {ProducerAcc, HookAcc}) ->
            {RuleProducers, ProcessedRules} = lists:foldl(
                fun({Filter, Key, Value, Topic, Seq, TopicProducerConf}, {PAcc, RAcc}) ->
                    Name = list_to_atom(lists:concat([atom_to_list(Hook), "_", Seq])),
                    case
                        get_or_create_producer(
                            ClientId, Topic, Name, ProducerConf, TopicProducerConf, ProducerCache
                        )
                    of
                        {ok, PayloadFormat, Sync, Timeout, Producers} ->
                            Rule = {Filter, Producers, Key, Value, PayloadFormat, Sync, Timeout},
                            {[Producers | PAcc], [Rule | RAcc]};
                        {error, _} ->
                            {PAcc, RAcc}
                    end
                end,
                {[], []},
                Rules
            ),
            %% 为这个hook点注册回调函数，传入所有规则
            case ProcessedRules of
                [] ->
                    {ProducerAcc, HookAcc};
                _ ->
                    load_(Hook, ProcessedRules),
                    {RuleProducers ++ ProducerAcc, [{Hook, ProcessedRules} | HookAcc]}
            end
        end,
        {[], []},
        GroupedHooks
    ),

    %% 清理缓存表
    ets:delete(ProducerCache),

    logger:info("~s is loaded.~n", [emqx_plugin_kafka]),
    {ok, ClientId, NProducers, ProcessedHooks}.

load_(Hook, Rules) ->
    case Hook of
        'client.connect' ->
            hook(Hook, {?MODULE, on_client_connect, [Rules]});
        'client.connack' ->
            hook(Hook, {?MODULE, on_client_connack, [Rules]});
        'client.connected' ->
            hook(Hook, {?MODULE, on_client_connected, [Rules]});
        'client.disconnected' ->
            hook(Hook, {?MODULE, on_client_disconnected, [Rules]});
        'client.authenticate' ->
            hook(Hook, {?MODULE, on_client_authenticate, [Rules]});
        'client.authorize' ->
            hook(Hook, {?MODULE, on_client_authorize, [Rules]});
        'client.subscribe' ->
            hook(Hook, {?MODULE, on_client_subscribe, [Rules]});
        'client.unsubscribe' ->
            hook(Hook, {?MODULE, on_client_unsubscribe, [Rules]});
        'session.created' ->
            hook(Hook, {?MODULE, on_session_created, [Rules]});
        'session.subscribed' ->
            hook(Hook, {?MODULE, on_session_subscribed, [Rules]});
        'session.unsubscribed' ->
            hook(Hook, {?MODULE, on_session_unsubscribed, [Rules]});
        'session.resumed' ->
            hook(Hook, {?MODULE, on_session_resumed, [Rules]});
        'session.discarded' ->
            hook(Hook, {?MODULE, on_session_discarded, [Rules]});
        'session.takenover' ->
            hook(Hook, {?MODULE, on_session_takenover, [Rules]});
        'session.terminated' ->
            hook(Hook, {?MODULE, on_session_terminated, [Rules]});
        'message.publish' ->
            hook(Hook, {?MODULE, on_message_publish, [Rules]});
        'message.delivered' ->
            hook(Hook, {?MODULE, on_message_delivered, [Rules]});
        'message.acked' ->
            hook(Hook, {?MODULE, on_message_acked, [Rules]});
        'message.dropped' ->
            hook(Hook, {?MODULE, on_message_dropped, [Rules]});
        _ ->
            logger:warning("Unknown hook: ~p", [Hook])
    end.

unload(HookList) ->
    lists:foreach(
        fun({Hook, _}) ->
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
    logger:debug("hook ~p with MFA ~p", [HookPoint, MFA]),
    emqx_hooks:add(HookPoint, MFA, _Property = ?HP_HIGHEST).

unhook(HookPoint, MFA) ->
    logger:debug("unhook ~p with MFA ~p", [HookPoint, MFA]),
    emqx_hooks:del(HookPoint, MFA).

on_client_connect(ClientInfo, Props, Rules) ->
    send_all(ClientInfo, Rules),
    {ok, Props}.

on_client_connack(ClientInfo, _Rc, Props, Rules) ->
    send_all(ClientInfo, Rules),
    {ok, Props}.

on_client_connected(ClientInfo, _ConnInfo, Rules) ->
    send_all(ClientInfo, Rules),
    ok.

on_client_disconnected(ClientInfo, _Reason, _ConnInfo, Rules) ->
    send_all(ClientInfo, Rules),
    ok.

on_client_authenticate(ClientInfo, AuthNResult, Rules) ->
    send_all(ClientInfo, Rules),
    {ok, AuthNResult}.

on_client_authorize(ClientInfo, _PubSub, _Topic, AuthNResult, Rules) ->
    send_all(ClientInfo, Rules),
    {ok, AuthNResult}.

on_client_subscribe(ClientInfo, _Props, TopicFilters, Rules) ->
    send_all(ClientInfo, Rules),
    {ok, TopicFilters}.

on_client_unsubscribe(ClientInfo, _Props, TopicFilters, Rules) ->
    send_all(ClientInfo, Rules),
    {ok, TopicFilters}.

on_session_created(ClientInfo, _SessInfo, Rules) ->
    send_all(ClientInfo, Rules),
    ok.

on_session_subscribed(ClientInfo, Topic, Opts, Rules) ->
    send_all(ClientInfo, Topic, maps:get(qos, Opts, 0), Rules),
    ok.

on_session_unsubscribed(ClientInfo, Topic, Opts, Rules) ->
    send_all(ClientInfo, Topic, maps:get(qos, Opts, 0), Rules),
    ok.

on_session_resumed(ClientInfo, _SessInfo, Rules) ->
    send_all(ClientInfo, Rules),
    ok.

on_session_discarded(ClientInfo, _SessInfo, Rules) ->
    send_all(ClientInfo, Rules),
    ok.

on_session_takenover(ClientInfo, _SessInfo, Rules) ->
    send_all(ClientInfo, Rules),
    ok.

on_session_terminated(ClientInfo, _Reason, _SessInfo, Rules) ->
    send_all(ClientInfo, Rules),
    ok.

on_message_publish(Msg, _Rules) when is_record(Msg, message) ->
    case emqx_message:topic(Msg) of
        <<"$SYS/", _/binary>> ->
            {ok, Msg};
        _ ->
            send_all(Msg, _Rules),
            {ok, Msg}
    end;
on_message_publish(Msg, Rules) ->
    send_all(Msg, Rules),
    {ok, Msg}.

on_message_delivered(_ClientInfo, Msg, Rules) ->
    send_all(Msg, Rules),
    {ok, Msg}.

on_message_acked(_ClientInfo, Msg, Rules) ->
    send_all(Msg, Rules),
    ok.

on_message_dropped(Msg, _By, _Reason, _Rules) when is_record(Msg, message) ->
    case emqx_message:topic(Msg) of
        <<"$SYS/", _/binary>> ->
            ok;
        _ ->
            send_all(Msg, _Rules),
            ok
    end;
on_message_dropped(Msg, _By, _Reson, Rules) ->
    send_all(Msg, Rules),
    ok.

parse_hook(Hooks) ->
    HookList = parse_hook(Hooks, [], 0),
    group_hooks_by_point(HookList).

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

%% 将同一hook点的规则分组
group_hooks_by_point(HookList) ->
    HookMap = lists:foldl(
        fun({Hook, Filter, Key, Value, Topic, Seq, ProducerConf}, Acc) ->
            Rule = {Filter, Key, Value, Topic, Seq, ProducerConf},
            case maps:get(Hook, Acc, undefined) of
                undefined ->
                    maps:put(Hook, [Rule], Acc);
                ExistingRules ->
                    maps:put(Hook, [Rule | ExistingRules], Acc)
            end
        end,
        #{},
        HookList
    ),
    maps:to_list(HookMap).

format_value_pattern(undefined) ->
    [];
format_value_pattern(Value) ->
    emqx_placeholder:preproc_tmpl(Value).

%% 发送消息到所有匹配的规则
send_all(Data, Rules) ->
    lists:foreach(
        fun(Rule) ->
            send(Data, Rule)
        end,
        Rules
    ).

%% 带Topic和Qos的发送函数
send_all(ClientInfo, Topic, Qos, Rules) ->
    lists:foreach(
        fun(Rule) ->
            send(ClientInfo, Topic, Qos, Rule)
        end,
        Rules
    ).

%% 单个规则的发送函数 - 处理ClientInfo
send(
    #{clientid := ClientId, username := Username},
    {_, Producers, Key, Value, _, Sync, Timeout}
) ->
    logger:debug("send client info (clientid: ~p, username: ~p) to kafka", [ClientId, Username]),
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
%% 单个规则的发送函数 - 处理Message
send(Msg, {Filter, Producers, Key, Value, PayloadFormat, Sync, Timeout}) when
    is_record(Msg, message)
->
    Topic = emqx_message:topic(Msg),
    logger:debug("send messag(topic:  ~p, message: ~p) to kafka, filter: ~p match? ~p", [
        Topic, emqx_message:payload(Msg), Filter, emqx_topic:match(Topic, Filter)
    ]),
    case emqx_topic:match(Topic, Filter) of
        true ->
            From = emqx_message:from(Msg),
            Payload = emqx_message:payload(Msg),
            case erlang:function_exported(emqx_message, headers, 1) of
                true ->
                    Headers = emqx_message:headers(Msg);
                false ->
                    Headers = emqx_message:get_headers(Msg)
            end,
            Qos = emqx_message:qos(Msg),
            Ts = emqx_message:timestamp(Msg),
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

%% 带Topic和Qos的单个规则发送函数
send(ClientInfo, Topic, Qos, {Filter, Producers, Key, Value, _, Sync, Timeout}) ->
    logger:debug("send Topic Info ~p ~p to kafka, filter: ~p match? ~p", [
        Topic, Qos, Filter, emqx_topic:match(Topic, Filter)
    ]),
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
