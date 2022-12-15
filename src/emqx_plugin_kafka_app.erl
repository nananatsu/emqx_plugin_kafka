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

start(_StartType, _StartArgs) ->
    Path = string:concat(code:priv_dir(emqx_plugin_kafka), "/config.hocon"),
    {_, Conf} = hocon:load(Path, #{format => richmap}),
    try hocon_tconf:generate(emqx_plugin_kafka_schema, Conf, #{}) of
        Props ->
            KafkaProps = proplists:get_value(kafka, Props),
            logger:debug("conf ~p ~n", [KafkaProps]),

            {ok, Sup} = emqx_plugin_kafka_sup:start_link(),
            case emqx_plugin_kafka_hook:load(KafkaProps) of
                {ok, ClientId, [], _} ->
                    logger:error("start emqx_plugin_kafka fail"),
                    wolff:stop_and_delete_supervised_client(ClientId);
                {ok, ClientId, NProducers, HookList} ->
                    logger:info("start emqx_plugin_kafka success"),
                    {ok, Sup, #{client_id => ClientId, n_producers => NProducers, hook => HookList}}
            end
    catch
        throw:{Schema, Errors} ->
            logger:error("generate kafka config fail ~p ~p ~n", Schema, Errors)
    end.

stop(#{client_id := ClientId, n_producers := NProducers, hook := HookList}) ->
    logger:info("###### stop kafka plugin "),
    emqx_plugin_kafka_hook:unload(HookList),

    logger:info("~p is unloaded.~n", [HookList]),
    lists:foreach(
        fun(Producers) ->
            wolff:stop_and_delete_supervised_producers(Producers)
        end,
        NProducers
    ),

    logger:info("NProducers closed ~p ~n ", [NProducers]),
    ok = wolff:stop_and_delete_supervised_client(ClientId).
