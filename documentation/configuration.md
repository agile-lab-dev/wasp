# Configurations

This document describes the configurations available for WASP, broken down by feature/component.

## Telemetry

The model is `it.agilelab.bigdata.wasp.models.configuration.TelemetryConfigModel`.

The typesafe config object is found at path `wasp.telemetry`.

The available configurations are:

| Model field | Configuration key | Optional | Default value | Description |
|---|---|---|---|---|
| writer | writer | no | default | Name of the datastore product to use, for example "solr" or "elastic" for a specific product, or a generic "default" or empty string to leave the choice to the framework | 
| sampleOneMessageEvery | latency.sample-one-message-every | no | 100 | Latency calculation sample rate, expressed as "1 in n", eg 100 => 1 in 100 => 1% sampling rate |


## Kafka multi cluster
In the scenario of a WASP instance that uses more than a single Kafka cluster, it is necessary to provide the additional configurations necessary to use the additional clusters.
To do this we can define the presence of additional Kafka clusters in our configurations as follows:
In the following example we associate the name 'kafka2' with the configurations, which will then be the reference used to specify which cluster a certain topic is located on.
The configurations are equivalent to those used to specify the main/default kafka configurations
```conf
additional-kafka-clusters {
    kafka2 {
            connections = [{
            protocol = ""
            host = ${HOSTNAME}
            port = 9093
            timeout = ${wasp.services-timeout-millis}
            metadata = []
        }]
        zookeeperConnections = [{
            protocol = ""
            host = ${HOSTNAME}
            port = 2181
            timeout = ${wasp.services-timeout-millis}
            metadata = []
        }]
        zkChRoot = "/kafka"
        ingest-rate = "1s"
        broker-id = 0
        partitioner-fqcn = "org.apache.kafka.clients.producer.internals.DefaultPartitioner"
        default-encoder = "kafka.serializer.DefaultEncoder"
        key-encoder-fqcn = "org.apache.kafka.common.serialization.ByteArraySerializer"
        encoder-fqcn = "org.apache.kafka.common.serialization.ByteArraySerializer"
        decoder-fqcn = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
        batch-send-size = 0
        acks = -1 
    }
}
```
It therefore becomes necessary to associate the topics with the correct cluster to which they belong; to do this, the alias cluster inserted in the previous configurations must be added to the topics that are to be declared.
The `it.agilelab.bigdata.wasp.models.TopicModel` case class provides an optional `clusterAlias` field which, if not specified, associates the topic with the main/default cluster, otherwise it is necessary to initialize it with the name of the kafka cluster that we specify in the conf within additional-kafka-clusters

