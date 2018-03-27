# WASP RESTful APIs
This documentation explain all the exposed services of WASP system. Via the APIs is possible to access and manage every element of the system. All the APIs return a response using JSON format, like below. At the moment is the only supported format.
This is the first version of this documentation and the APIs are currently work in progress. In the next future we will make improvements at the system and consequently at the APIs.
Below you can find all the APIs associated with the supported HTTP verbs.

## Pipegraphs

|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /pipegraphs  |  Get all the pipegraph in the system. | Insert a new pipegraph.  | Update an existing pipegraph.  |  |
| /pipegraphs/{id}  | Get the pipegraph with the specified id. |   |   | Delete the pipegraph with the specified id.  |
| /pipegraphs/name/{name} | Get the pipegraph with the specified name. |   |   |   |
| /pipegraphs/{id}/start |  | Start the pipegraph with the specified id. |   |   |
| /pipegraphs/{id}/stop |  | Stop the pipegraph with the specified id. |   |   |
| /pipegraphs/{id}/instances |  Retrieve instances of specified pipegraph ordered newest to latest |   |   | |

Pipegraphs Sample

`GET http://localhost:2891/pipegraphs`

```javascript
{
  "Result": "OK",
  "data": [
    {
      "name": "MetroPipegraph6",
      "description": "Los Angeles Metro Pipegraph",
      "owner": "user",
      "system": false,
      "creationTime": 1475066851495,
      "etl": [
        {
          "name": "write on index",
          "inputs": [
            {
              "id": {
                "$oid": "57ebbbe30100000100835289"
              },
              "name": "metro.topic",
              "readerType": "topic"
            }
          ],
          "output": {
            "id": {
              "$oid": "57ebbbe3010000010083528a"
            },
            "name": "metro_index",
            "writerType": {
              "wtype": "index",
              "product": "elastic"
            }
          },
          "mlModels": [],
          "strategy": {
            "className": "it.agilelab.bigdata.wasp.pipegraph.metro.strategies.MetroStrategy"
          },
          "group": "default",
          "isActive": false
        }
      ],
      "rt": [],
      "isActive": false,
      "_id": {
        "$oid": "57ebbbe3010000010083528b"
      }
    }
  ]
}
```

Start a producer with the system id `57ebbbe3010000010083528b`. 

`POST http://localhost:2891/producers/57ebbbe3010000010083528b/start`
```javascript
{
  "Result": "OK",
  "data": "Producer 'MetroProducer' started"
}
```

## Producers
|   |  GET |  POST | PUT | DELETE  |
|---|---|---|---|---|
| /producers |  Get all the procuders in the system. |   | Update an existing pipegraph.  |   |
| /producers/{id} | Get the producer with the specified id. |   |   |   |
| /producers/{id}/start |  | Start the producer with the specified id. |   |   |
| /producers/{id}/stop |  | Stop the producer with the specified id. |   |   |

Producers Sample

`GET http://localhost:2891/producers`

```javascript
{
  "Result": "OK",
  "data": [
    {
      "name": "MetroProducer",
      "className": "it.agilelab.bigdata.wasp.pipegraph.metro.producers.MetroProducer",
      "id_topic": {
        "$oid": "57ebbbe30100000100835289"
      },
      "isActive": false,
      "_id": {
        "$oid": "57ebbbe3010000010083528c"
      }
    }
  ]
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
| /batchjobs | Get all the batchjobs in the system. | Insert a new batchjobs.  | Update an existing batchjobs. | |
| /batchjobs/{id} | Get the batchjob with the specified id. | | | Delete the batchjobs with the specified id. |
| /batchjobs/{id}/start | | Start a instance of specified job with optional JSON configuration. | | |
| /batchjobs/{id}/instances | Retrieve instances of specified job ordered newest to latest. | | | |

Batchjobs Sample

`GET http://localhost:2891/batchjobs`

```javascript
{
  "Result": "OK",
  "data": [
    {
      "name": "BatchJobExample",
      "description": "BatchJob example",
      "owner": "system",
      "system": true,
      "creationTime": 1475220991703,
      "etl": {
        "name": "WriteToBatchOutput",
        "inputs": [
          {
            "id": {
              "$oid": "57ee15ff5a77b16a008d30cb"
            },
            "name": "batchoutput_index",
            "readerType": "index"
          }
        ],
        "output": {
          "id": {
            "$oid": "57ee15ff5a77b16a008d30cb"
          },
          "name": "batchoutput_index",
          "writerType": {
            "wtype": "index",
            "product": "elastic"
          }
        },
        "mlModels": [],
        "strategy": {
          "className": "it.agilelab.bigdata.wasp.batch.strategies.DummyBatchStrategy"
        },
        "group": "default",
        "isActive": true
      },
      "state": "PENDING",
      "_id": {
        "$oid": "57ee15ff5a77b16a008d30cc"
      }
    },
    {
      "name": "BatchJobWithModelCreationExample",
      "description": "BatchJobWithModelCreationExample example",
      "owner": "system",
      "system": true,
      "creationTime": 1475220991748,
      "etl": {
        "name": "empty",
        "inputs": [],
        "output": {
          "id": {
            "$oid": "57ee15ff5a77b16a008d30cb"
          },
          "name": "batchoutput_index",
          "writerType": {
            "wtype": "index",
            "product": "elastic"
          }
        },
        "mlModels": [],
        "strategy": {
          "className": "it.agilelab.bigdata.wasp.batch.strategies.BatchModelMaker"
        },
        "group": "default",
        "isActive": true
      },
      "state": "PENDING",
      "_id": {
        "$oid": "57ee15ff5a77b16a008d30cd"
      }
    }
  ]
}
```

Batchjob instance start Sample
```bash
curl -X POST \
  http://localhost:2891/batchjobs/TestBatchJobFromHdfsFlatToConsole/start \
  -H 'Content-Type: application/json' \
  -d '{
        "stringKey": "aaa",
        "intKey2": 5
     }'
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
| /configs/es  | Get the Elasticsearch configuration. If exists. |   |   |   |
| /configs/solr  | Get the Solr configuration. If exists. |   |   |   |

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