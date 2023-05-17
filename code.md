1. Emqx Plugin Kafka插件使用[emqx_plugin_template](https://github.com/emqx/emqx-plugin-template)作为基础模板，在此基础上进行开发  
- emqx_plugin_kafka_app.erl 插件启动
- emqx_plugin_kafka_hook.erl emqx hook挂载
- emqx_plugin_kafka_schema.erl 配置文件格式
- emqx_plugin_kafka.erl kafka连接/消息发送

2. 插件启动流程:
- emqx_plugin_kafka_app:start() 加载配置文件，按emqx_plugin_kafka_schema解析为hocon,成功后调用emqx_plugin_kafka_hook:load()
- emqx_plugin_kafka_hook:load() 调用emqx_plugin_kafka_app:start()，成功后挂载emqx hook，hook触发时调用emqx_plugin_kafka:send_msg_to_kafka()发送指定格式消息到kafka指定topic
- emqx_plugin_kafka:start_producer() 接收hocon配置解析，启动woff kafka producer进程

3. 插件配置分为三个部分
- kafka客户端配置，同woff客户端配置一致
- kafka生产者配置，同woff生产者配置一致
- emqx钩子配置，配置需要挂载的emqx钩子
    - hook 为emqx钩子挂载点
    - filter 为需要转发到emqx的mqtt主题，调用emqx_topic:match()进行匹配
    - topic 为要发送到的kafka主题
    - key 为kafka消息key模板，调用emqx_placeholder:preproc_tmpl()解析模板，可填充clientid、from、username、topic、payload、qos、node、ts
    - value 为kafka消息value模板，调用emqx_placeholder:preproc_tmpl()解析模板，可填充clientid、from、username、topic、payload、qos、node、ts

4. 在emqx_plugin_kafka_schema.erl中按设计插件配置格式定义hocon    

```erlang
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
```

5. 实现配置文件加载解析

```erlang
load_config(File) ->
    {_, Conf} = hocon:load(File, #{format => richmap}),
    try hocon_tconf:generate(emqx_plugin_kafka_schema, Conf, #{}) of
        Props ->
            KafkaConf = proplists:get_value(kafka, Props),

            ClientConf = get_value(client, KafkaConf, #{}),
            ProducerConf = get_value(producer, KafkaConf, #{}),
            HooKConf = get_value(hook, KafkaConf, []),
            {ok, ClientConf, ProducerConf, HooKConf}
    catch
        throw:{Schema, Errors} ->
            logger:error("load kafka config fail ~p ~p ~n", Schema, Errors),
            {error, Errors}
    end.
```

6. 读取客户端配置,启动kafka客户端进程

```erlang
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
                    [{sasl,  ...} | Acc ];
                <<"ssl">> ->
                    [  {ssl, ... } | Acc ];
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
```

7. 读取生产者配置,启动kafka生产者进程  

```erlang
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
```

8. 实现解析hocon钩子配置，调用emqx:hook()注册钩子回调函数

```erlang
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

load_(Hook, Params) ->
    case Hook of
        ...
        'message.publish' ->
            hook(Hook, {?MODULE, on_message_publish, [Params]});
        'message.delivered' ->
            hook(Hook, {?MODULE, on_message_delivered, [Params]});
        'message.acked' ->
            hook(Hook, {?MODULE, on_message_acked, [Params]});
        'message.dropped' ->
            hook(Hook, {?MODULE, on_message_dropped, [Params]})
    end.
```
9. 实现钩子回调函数，在回调函数中填充kafka消息模板发送

```erlang
on_message_publish(Msg = #message{topic = <<"$SYS/", _/binary>>}, _Rule) ->
    {ok, Msg};
on_message_publish(Msg, Rule) ->
    send(Msg, Rule),
    {ok, Msg}.

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

```
10. 在启动函数调用相关函数，实现配置加载->按配置连接kafka->注册配置的钩子
```erlang
start(_StartType, _StartArgs) ->
    ConfFile = "/opt/emqx/etc/emqx_plugin_kafka.conf",
    DefaultConfFile = string:concat(code:priv_dir(emqx_plugin_kafka), "/config.hocon"),

    case
        case filelib:is_regular(ConfFile) of
            true ->
                load_config(ConfFile);
            false ->
                case filelib:is_regular(DefaultConfFile) of
                    true -> load_config(DefaultConfFile);
                    false -> {ok, #{}, #{}, []}
                end
        end
    of
        {ok, ClientConf, ProducerConf, HooKConf} ->
            logger:debug("ClientConf ~p ,ProducerConf: ~p ,HooKConf: ~p ~n", [
                ClientConf, ProducerConf, HooKConf
            ]),

            {ok, Sup} = emqx_plugin_kafka_sup:start_link(),
            case emqx_plugin_kafka_hook:load(ClientConf, ProducerConf, HooKConf) of
                {ok, ClientId, [], _} ->
                    logger:info("start emqx_plugin_kafka fail"),
                    wolff:stop_and_delete_supervised_client(ClientId),
                    {error, "no kafka producer configured"};
                {ok, ClientId, NProducers, HookList} ->
                    logger:info("start emqx_plugin_kafka success"),
                    {ok, Sup, #{client_id => ClientId, n_producers => NProducers, hook => HookList}}
            end
    end.

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

```