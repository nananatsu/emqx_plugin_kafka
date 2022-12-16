# emqx-plugin-kafka

This is a template plugin for EMQX >= 5.0.

For EMQX >= 4.3, please see branch emqx-v4

For older EMQX versions, plugin development is no longer maintained.

## Release

A EMQX plugin release is a zip package including

1. A JSON format metadata file
2. A tar file with plugin's apps packed

Execute `make rel` to have the package created like:

```
_build/default/emqx_plugrel/emqx_plugin_kafka-<vsn>.tar.gz
```
See EMQX documents for details on how to deploy the plugin.

Plugin read config from /opt/emqx/etc/emqx_plugin_kafka.conf, exmple:

>   
    kafka {
    client  {
        client_id = "emqx_kafka"
        servers = [ "localhost:9092"]
        connection_strategy = per_partition
        min_metadata_refresh_interval = 5000
        query_api_versions = true
        request_timeout = 240000
    }
    producer {
        produce = sync
        produce_sync_timeout = 3000
        encode_payload_type = plain
        replayq_dir = undefined
        compression = no_compression
        partitioner = random
    }
    hook = [
        {
         hook = "message.publish"
         filter = "test/#"
         topic = "test"
         key = "${topic}"
         value = "{ \"topic\" : \"${topic}\", \"payload\" : \"${payload}\", \"ts\" : \"${ts}\" }"
         producer {
            produce = sync
            produce_sync_timeout = 3000
            encode_payload_type = plain
            replayq_dir = undefined
            compression = no_compression
            partitioner = random
         }
        }
    ] 
}
