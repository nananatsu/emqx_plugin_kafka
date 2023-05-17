# emqx-plugin-kafka

This is a kafka plugin for EMQX >= 5.0.

[代码说明](./code.md)

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
        ## required
        client  {
            ## required
            client_id = "emqx_kafka"
            ## required
            servers = [ "localhost:9092" ]
            ## allow: per_partition | per_broker
            ## default: per_partition
            connection_strategy = per_partition
            ## unit: ms
            ## default: 5000
            min_metadata_refresh_interval = 5000
            ## allow: boolean
            ## default: true
            query_api_versions = true
            ## unit: ms
            ## default: 240000
            request_timeout = 240000
            ## optional
            ## sasl  { 
            ##     ## allow: plain | scram_sha_256 | scram_sha_512
            ##     mechanism = plain
            ##     username = "username"
            ##     password = "password"
            ##  }
            ## optional
            ##  ssl {
            ##     ## allow: verify_none | verify_none
            ##     verify = verify_none
            ##     ca_cert_file = "/etc/ssl/cert.pem"
            ##     depth = 3
            ##     ##  enable support for common wildcard certificates
            ##     customize_hostname_check = true
            ##  }
        }
        ## required
        ## global kafka producer config
        producer {
            ## allow: sync | async
            ## default: sync
            produce = sync
            ## unit: ms
            ## default: 3000
            produce_sync_timeout = 3000
            ## mqtt payload encode for kafak
            ## allow: plain | base64
            ## default: plain
            encode_payload_type = plain
            ## Base directory for replayq to store messages on disk
            ## default: undefined
            replayq_dir = undefined
            ## allow: no_compression | snappy | gzip
            ## default: no_compression
            compression = no_compression
            ## allow: random | roundrobin
            partitioner = random
        }
        ## required
        ## emqx hook config
        hook = [
            {
            ## hook point 
            hook = "message.publish"
            ## emqx topic pattern
            filter = "test/#"
            ## kafka topic
            topic = "test"
            ## kafka key pattern
            key = "${topic}"
            ## kafka value pattern
            value = "{ \"topic\" : \"${topic}\", \"payload\" : \"${payload}\", \"ts\" : \"${ts}\" }"
            ## optional
            ## kafka producer config
            ## producer {
            ##    produce = sync
            ##    produce_sync_timeout = 3000
            ##    encode_payload_type = plain
            ##    replayq_dir = undefined
            ##    compression = no_compression
            ##    partitioner = random
            ## }
            }
        ] 
    }

