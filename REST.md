# WASP RESTful APIs
This documentation explain all the exposed services of WASP system. Via the APIs is possible to access and manage every element of the system. All the APIs return a response using JSON format, like below. At the moment is the only supported format.
This is the first version of this documentation and the APIs are currently work in progress. In the next future we will make improvements at the system and consequently at the APIs.
Below you can find all the APIs associated with the supported HTTP verbs.

## Pipegraphs

|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /pipegraphs  |  Get all pipegraphs in the system | Insert a new pipegraph  | Update an existing pipegraph  |  |
| /pipegraphs/{id}  | Get the pipegraph with the specified id |   |   | Delete the pipegraph with the specified id  |
| /pipegraphs/{id}/start |  | Start an instance of the specified pipegraph and return the related instance id |   |   |
| /pipegraphs/{id}/stop |  | Stop the active (i.e. "PROCESSING") instance of the specified pipegraph |   |   |
| /pipegraphs/{id}/instances |  Retrieve instances of the specified pipegraph, ordered newest to latest |   |   | |
| /pipegraphs/{pipegraphId}/instances/{instanceId} |  Retrieve instance status of the specified pipegraph instance |   |   | |

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
| /topics |  Get all the topics in the system. |   |   |   |
| /topics/{id} | Get the producer with the specified id.  |   |   |   |


Topics Sample

`GET http://localhost:2891/topics`

```javascript
{
  "Result": "OK",
  "data": [
    {
      "name": "metro.topic",
      "creationTime": 1475066850990,
      "schema": {
        "type": "record",
        "namespace": "PublicTransportsTracking",
        "name": "LAMetro",
        "fields": [
          {
            "name": "id_event",
            "type": "double"
          },
          {
            "name": "source_name",
            "type": "string"
          },
          {
            "name": "topic_name",
            "type": "string"
          },
          {
            "name": "metric_name",
            "type": "string"
          },
          {
            "name": "timestamp",
            "type": "string",
            "java-class": "java.util.Date"
          },
          {
            "name": "latitude",
            "type": "double"
          },
          {
            "name": "longitude",
            "type": "double"
          },
          {
            "name": "value",
            "type": "double"
          },
          {
            "name": "payload",
            "type": "string"
          },
          {
            "name": "last_update",
            "type": "double"
          },
          {
            "name": "vehicle_code",
            "type": "int"
          },
          {
            "name": "position",
            "type": {
              "name": "positionData",
              "type": "record",
              "fields": [
                {
                  "name": "lat",
                  "type": "double"
                },
                {
                  "name": "lon",
                  "type": "double"
                }
              ]
            }
          },
          {
            "name": "predictable",
            "type": "boolean"
          }
        ]
      },
      "_id": {
        "$oid": "57ebbbe30100000100835289"
      }
    }
  ]
}
```

## Batchjobs
|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /batchjobs | Get all batchjobs in the system | Insert a new batchjob  | Update an existing batchjob | |
| /batchjobs/{id} | Get the batchjob with the specified id | | | Delete the batchjobs with the specified id |
| /batchjobs/{id}/start | | Start an instance  (with optional JSON configuration) of the specified batchjob and return the related instance id | | |
| /batchjobs/{id}/instances | Retrieve instances of the specified batchjob, ordered newest to latest | | | |
| /batchjobs/{batchjobId}/instances/{instanceId} | Retrieve instance status of the specified batchjob instance | | | |

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
| /index/{name} | Get the pipegraph with the specified name. |   |   |   |

Indices Sample

`GET http://localhost:2891/index/metro_index`

```javascript
{
  "Result": "OK",
  "data": {
    "name": "metro_index",
    "creationTime": 1475161139634,
    "schema": {
      "lametro": {
        "properties": {
          "id_event": {
            "type": "double",
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          },
          "source_name": {
            "type": "string",
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          },
          "Index_name": {
            "type": "string",
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          },
          "metric_name": {
            "type": "string",
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          },
          "timestamp": {
            "type": "date",
            "format": "date_time",
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          },
          "latitude": {
            "type": "double",
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          },
          "longitude": {
            "type": "double",
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          },
          "value": {
            "type": "double",
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          },
          "payload": {
            "type": "string",
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          },
          "last_update": {
            "type": "double",
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          },
          "vehicle_code": {
            "type": "integer",
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          },
          "position": {
            "type": "geo_point",
            "geohash": true,
            "geohash_prefix": true,
            "geohash_precision": 7,
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          },
          "predictable": {
            "type": "boolean",
            "index": "not_analyzed",
            "store": "true",
            "enabled": "true"
          }
        }
      }
    },
    "_id": {
      "$oid": "57ed2c3344000001007f27b6"
    },
    "numShards": 2,
    "replicationFactor": 1
  }
}
```

## ML Models
|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /mlmodels | Get all the ML models in the system. |   | Update an existing ML models.  |   |
| /mlmodels/{id} | Get the ML models with the specified id. |   |   | Delete the ML models with the specified id. |

## Configurations
|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /configs/kafka  | Get the Kakfa configuration. |   |   |   |
| /configs/sparkbatch  | Get the Spark batch configuration. |   |   |   |
| /configs/sparkstreaming  | Get the Spark streaming configuration. |   |   |   |
| /configs/es  | Get the Elasticsearch configuration, if exists. |   |   |   |
| /configs/solr  | Get the Solr configuration, if exists. |   |   |   |

Configurations Sample

`GET http://localhost:2891/configs/kafka`

```javascript
{
  "Result": "OK",
  "data": {
    "connections": [
      {
        "protocol": "",
        "host": "server02.cluster01.atscom.it",
        "port": 9092,
        "timeout": 5000
      }
    ],
    "ingest_rate": "1s",
    "zookeeper": {
      "protocol": "",
      "host": "server01.cluster01.atscom.it",
      "port": 2181,
      "timeout": 5000
    },
    "broker_id": "0",
    "partitioner_fqcn": "kafka.producer.DefaultPartitioner",
    "default_encoder": "kafka.serializer.DefaultEncoder",
    "encoder_fqcn": "kafka.serializer.StringEncoder",
    "decoder_fqcn": "kafka.serializer.StringDecoder",
    "batch_send_size": 100,
    "name": "Kafka"
  }
}
```