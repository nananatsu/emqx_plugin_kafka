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

-module(emqx_plugin_kafka_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-include_lib("emqx/include/logger.hrl").

-export([start/2, stop/1]).

-import(proplists, [get_value/2, get_value/3]).

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

stop(#{client_id := ClientId, n_producers := NProducers, hook := HookList}) ->
    emqx_plugin_kafka_hook:unload(HookList),

    lists:foreach(
        fun(Producers) ->
            wolff:stop_and_delete_supervised_producers(Producers)
        end,
        NProducers
    ),

    ok = wolff:stop_and_delete_supervised_client(ClientId).

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
