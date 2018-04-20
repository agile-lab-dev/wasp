# WASP RESTful APIs
This documentation explains all the exposed services of WASP system. Via the APIs is possible to access and manage every element of the system.
All APIs return a response using JSON format (at the moment this is the only supported format).

This is the first version of this documentation and the APIs are currently work in progress. In the next future we will make improvements at the system and consequently at the APIs.

Below you can find all the APIs associated with the supported HTTP verbs.

## Pipegraphs

|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /pipegraphs  |  Get all the pipegraphs in the system | Insert a new pipegraph  | Update an existing pipegraph  |  |
| /pipegraphs/{id}  | Get the pipegraph with the specified id |   |   | Delete the pipegraph with the specified id  |
| /pipegraphs/{id}/start |  | Start an instance of the specified pipegraph and return the related instance id |   |   |
| /pipegraphs/{id}/stop |  | Stop the active (i.e. "PROCESSING") instance of the specified pipegraph |   |   |
| /pipegraphs/{id}/instances |  Get instances of the specified pipegraph, ordered newest to latest |   |   | |
| /pipegraphs/{pipegraphId}/instances/{instanceId} |  Get instance status of the specified pipegraph instance |   |   | |

Pipegraphs

`GET http://localhost:2891/pipegraphs`
```javascript
{
    "Result": "OK",
    "data": [
        {
            "isSystem": true,
            "name": "LoggerPipegraph",
            "description": "System Logger Pipegraph",
            "legacyStreamingComponents": [],
            "structuredStreamingComponents": [
                {
                    "name": "write on index",
                    "config": {},
                    "kafkaAccessType": "receiver-based",
                    "inputs": [
                        {
                            "name": "logger.topic",
                            "endpointName": "logger.topic",
                            "readerType": {
                                "category": "topic",
                                "product": "kafka"
                            }
                        }
                    ],
                    "output": {
                        "name": "logger_index",
                        "endpointName": "logger_index",
                        "writerType": {
                            "category": "index",
                            "product": "solr"
                        }
                    },
                    "mlModels": [],
                    "group": "default"
                }
            ],
            "rtComponents": [],
            "owner": "system",
            "creationTime": 1523434864847
        },
        ...
    ]
}
```

Pipegraph instance start

`POST http://localhost:2891/pipegraphs/TestConsoleWriterStructuredJSONPipegraph/start`
```javascript
{
    "Result": "OK",
    "data": {
        "startResult": "Pipegraph 'TestConsoleWriterStructuredJSONPipegraph' start accepted'",
        "instance": "TestConsoleWriterStructuredJSONPipegraph-6e139f53-254c-44b9-8b6a-3dbbaaa84760"
    }
}
```

Pipegraph instances status

`GET http://localhost:2891/pipegraphs/TestConsoleWriterStructuredJSONPipegraph/instances`
```javascript
{
    "Result": "OK",
    "data": [
        {
            "name": "TestConsoleWriterStructuredJSONPipegraph-6e139f53-254c-44b9-8b6a-3dbbaaa84760",
            "instanceOf": "TestConsoleWriterStructuredJSONPipegraph",
            "currentStatusTimestamp": 1523435670321,
            "status": "PROCESSING",
            "startTimestamp": 1523435670306
        },
        {
            "name": "TestConsoleWriterStructuredJSONPipegraph-8bab7248-2077-411b-ae43-e1ac6d9b6833",
            "instanceOf": "TestConsoleWriterStructuredJSONPipegraph",
            "currentStatusTimestamp": 1523435666550,
            "status": "STOPPED",
            "startTimestamp": 1523435661763
        }
    ]
}
```

Pipegraph specified instance status

`GET http://localhost:2891/pipegraphs/TestConsoleWriterStructuredJSONPipegraph/instances/TestConsoleWriterStructuredJSONPipegraph-6e139f53-254c-44b9-8b6a-3dbbaaa84760`
```javascript
{
    "Result": "OK",
    "data": {
            "name": "TestConsoleWriterStructuredJSONPipegraph-6e139f53-254c-44b9-8b6a-3dbbaaa84760",
            "instanceOf": "TestConsoleWriterStructuredJSONPipegraph",
            "currentStatusTimestamp": 1523435670321,
            "status": "PROCESSING",
            "startTimestamp": 1523435670306
    }
}
```

## Producers
|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /producers |  Get all the producers in the system |   | Update an existing producer  |   |
| /producers/{id} | Get the producer with the specified id |   |   |   |
| /producers/{id}/start |  | Start the producer with the specified id |   |   |
| /producers/{id}/stop |  | Stop the producer with the specified id |   |   |

Producers

`GET http://localhost:2891/producers`
```javascript
{
    "Result": "OK",
    "data": [
        {
            "isSystem": true,
            "name": "LoggerProducer",
            "isRemote": false,
            "className": "it.agilelab.bigdata.wasp.producers.InternalLogProducerGuardian",
            "topicName": "logger.topic",
            "isActive": false
        },
        ...
    ]
}
```

Producer start

`POST http://localhost:2891/producers/LoggerProducer/start`
```javascript
{
  "Result": "OK",
  "data": "Producer 'LoggerProducer' started"
}
```

## Topics
|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /topics |  Get all the topics in the system |   |   |   |
| /topics/{id} | Get the producer with the specified id  |   |   |   |


Topics

`GET http://localhost:2891/topics`
```javascript
{
    "Result": "OK",
    "data": [
        {
            "name": "logger.topic",
            "replicas": 1,
            "topicDataType": "avro",
            "schema": {
                "type": "record",
                "namespace": "logging",
                "name": "logging",
                "fields": [
                    {
                        "name": "log_source",
                        "type": "string",
                        "doc": "Class that logged this message"
                    },
                    {
                        "name": "log_level",
                        "type": "string",
                        "doc": "Logged message level"
                    },
                    {
                        "name": "message",
                        "type": "string",
                        "doc": "Logged message"
                    },
                    {
                        "name": "timestamp",
                        "type": "string",
                        "doc": "Logged message timestamp in  ISO-8601 format"
                    },
                    {
                        "name": "thread",
                        "type": "string",
                        "doc": "Thread that logged this message"
                    },
                    {
                        "name": "cause",
                        "type": "string",
                        "doc": "Message of the logged exception attached to this logged message"
                    },
                    {
                        "name": "stack_trace",
                        "type": "string",
                        "doc": "Stacktrace of the logged exception attached to this logged message"
                    }
                ]
            },
            "partitions": 3,
            "creationTime": 1523635138573
        },
        ...
```

## Batchjobs
|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /batchjobs | Get all the batchjobs in the system | Insert a new batchjob  | Update an existing batchjob | |
| /batchjobs/{id} | Get the batchjob with the specified id | | | Delete the batchjobs with the specified id |
| /batchjobs/{id}/start | | Start an instance  (with optional JSON configuration) of the specified batchjob and return the related instance id | | |
| /batchjobs/{id}/instances | Get instances of the specified batchjob, ordered newest to latest | | | |
| /batchjobs/{batchjobId}/instances/{instanceId} | Get instance status of the specified batchjob instance | | | |

Batchjobs

`GET http://localhost:2891/batchjobs`
```javascript
{
    "Result": "OK",
    "data": [
        {
            "name": "TestBatchJobFromElasticToHdfs",
            "system": false,
            "description": "Description pf TestBatchJobFromElasticToHdfs",
            "etl": {
                "name": "EtlModel for TestBatchJobFromElasticToHdfs",
                "kafkaAccessType": "direct",
                "inputs": [
                    {
                        "name": "Elastic Reader",
                        "endpointName": "test_elastic_index",
                        "readerType": {
                            "category": "index",
                            "product": "elastic"
                        }
                    }
                ],
                "strategy": {
                    "className": "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestIdentityStrategy",
                    "configuration": "{\"intKey\":1,\"stringKey\":\"stringValue\"}"
                },
                "isActive": false,
                "output": {
                    "name": "Raw Writer",
                    "endpointName": "TestRawNestedSchemaModel",
                    "writerType": {
                        "category": "raw",
                        "product": "raw"
                    }
                },
                "mlModels": [],
                "group": "default"
            },
            "owner": "user",
            "creationTime": 1523436991649
        },
        ...
    ]
}
```

Batchjob instance start

```bash
curl -X POST \
  http://localhost:2891/batchjobs/TestBatchJobFromHdfsFlatToConsole/start \
  -H 'Content-Type: application/json' \
  -d '{
        "stringKey": "aaa",
        "intKey2": 5
     }'
```
```javascript
{
    "Result": "OK",
    "data": {
        "startResult": "Batch job 'TestBatchJobFromHdfsFlatToConsole' start accepted'",
        "instance": "TestBatchJobFromHdfsFlatToConsole-8a900d14-3859-4a5a-b2c2-5b8fcb8250c4"
    }
}
```

Batchjob instances status

`GET http://localhost:2891/batchjobs/TestBatchJobFromHdfsFlatToConsole/instances`
```javascript
{
    "Result": "OK",
    "data": [
        {
            "name": "TestBatchJobFromHdfsFlatToConsole-8a900d14-3859-4a5a-b2c2-5b8fcb8250c4",
            "instanceOf": "TestBatchJobFromHdfsFlatToConsole",
            "restConfig": {
                "intKey2": 5,
                "stringKey": "aaa"
            },
            "currentStatusTimestamp": 1523437338661,
            "error": "java.lang.Exception: Failed to create data frames for job TestBatchJobFromHdfsFlatToConsole...",
            "status": "FAILED",
            "startTimestamp": 1523437333307
        },
        {
            "name": "TestBatchJobFromHdfsFlatToConsole-87503963-05b4-4fd5-a82f-1c0760c0ffac",
            "instanceOf": "TestBatchJobFromHdfsFlatToConsole",
            "restConfig": {
                "intKey2": 5,
                "stringKey": "aaa"
            },
            "currentStatusTimestamp": 1523437329580,
            "error": "java.lang.Exception: Failed to create data frames for job TestBatchJobFromHdfsFlatToConsole...",
            "status": "FAILED",
            "startTimestamp": 1523437326018
        }
    ]
}
```

Batchjob specified instance status

`GET http://localhost:2891/batchjobs/TestBatchJobFromHdfsFlatToConsole/instances/TestBatchJobFromHdfsFlatToConsole-8a900d14-3859-4a5a-b2c2-5b8fcb8250c4`
```javascript
{
    "Result": "OK",
    "data": {
        "name": "TestBatchJobFromHdfsFlatToConsole-8a900d14-3859-4a5a-b2c2-5b8fcb8250c4",
        "instanceOf": "TestBatchJobFromHdfsFlatToConsole",
        "restConfig": {
            "intKey2": 5,
            "stringKey": "aaa"
        },
        "currentStatusTimestamp": 1523437338661,
        "error": "java.lang.Exception: Failed to create data frames for job TestBatchJobFromHdfsFlatToConsole...",
        "status": "FAILED",
        "startTimestamp": 1523437333307
    }
}
```

## Indices
|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /indexes |  Get all the indexes in the system |   |   |   |
| /indexes/{id} | Get the index with the specified id |   |   |   |

Indices

`GET http://localhost:2891/indexes`
```javascript
{
    "Result": "OK",
    "data": [
        {
            "name": "logger_solr_index",
            "schema": "[{\"name\":\"log_source\",\"type\":\"string\",\"indexed\":true,\"stored\":true,\"required\":false},{\"name\":\"log_level\",\"type\":\"string\",\"indexed\":true,\"stored\":true,\"required\":false},{\"name\":\"message\",\"type\":\"string\",\"indexed\":true,\"stored\":true,\"required\":false},{\"name\":\"timestamp\",\"type\":\"date\",\"indexed\":true,\"stored\":true,\"required\":false},{\"name\":\"thread\",\"type\":\"string\",\"indexed\":true,\"stored\":true,\"required\":false},{\"name\":\"cause\",\"type\":\"string\",\"indexed\":true,\"stored\":true,\"required\":false},{\"name\":\"stack_trace\",\"type\":\"string\",\"indexed\":true,\"stored\":true,\"required\":false}]",
            "replicationFactor": 1,
            "rollingIndex": false,
            "creationTime": 1523635512255,
            "numShards": 1
        },
        ...
}
```

## ML Models
|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /mlmodels | Get all the ML models in the system |   | Update an existing ML models  |   |
| /mlmodels/{id} | Get the ML models with the specified id |   |   | Delete the ML models with the specified id |

## Configurations
|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /configs/kafka  | Get the Kakfa configuration |   |   |   |
| /configs/sparkbatch  | Get the Spark batch configuration |   |   |   |
| /configs/sparkstreaming  | Get the Spark streaming configuration |   |   |   |
| /configs/es  | Get the Elasticsearch configuration |   |   |   |
| /configs/solr  | Get the Solr configuration |   |   |   |

Configurations Sample

`GET http://localhost:2891/configs/kafka`
```javascript
{
    "Result": "OK",
    "data": {
        "key_encoder_fqcn": "org.apache.kafka.common.serialization.StringSerializer",
        "name": "Kafka",
        "zookeeperConnections": {
            "connections": [
                {
                    "host": "zookeeper",
                    "port": 2181,
                    "metadata": {},
                    "timeout": 15000,
                    "protocol": ""
                }
            ],
            "chRoot": "/kafka"
        },
        "batch_send_size": 0,
        "default_encoder": "kafka.serializer.DefaultEncoder",
        "broker_id": "0",
        "ingest_rate": "1s",
        "others": [],
        "partitioner_fqcn": "org.apache.kafka.clients.producer.internals.DefaultPartitioner",
        "acks": "-1",
        "decoder_fqcn": "org.apache.kafka.common.serialization.ByteArrayDeserializer",
        "encoder_fqcn": "org.apache.kafka.common.serialization.ByteArraySerializer",
        "connections": [
            {
                "host": "kafka",
                "port": 9092,
                "metadata": {},
                "timeout": 15000,
                "protocol": ""
            }
        ]
    }
}
```