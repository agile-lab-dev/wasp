# Operations

## Deploying and Starting
**using docker containers**
- Copy the full project repository in the machine
- Start all containers (WASP components) using the `docker/start-wasp.sh` specialization (e.g. `docker/start-whitelabel-wasp.sh`)
    - To start with a clean configuration, remove the MongoDB database `wasp`; this operation can be automatically done passing `-d` option to the script

**not using docker containers**
- ...

---

- Start all pipegraphs and check them using [Pipegraph APIs](api.md#pipegraphs)
    - Use the Spark Web UI to check the executors were started
- *[Optional] Start all producers and check them using [Producer APIs](api.md#producers)*

## Stopping
- Stop all pipegraphs and check them using [Pipegraph APIs](api.md#pipegraphs)
    - Use the Spark Web UI to check the executors were stopped
- *[Optional] Stop all producers and check them using [Producer APIs](api.md#producers)*

**using docker containers**
- Stop all containers (WASP components) using `docker/stop-wasp.sh`

**not using docker containers**
- ...

## Standard Logging
WASP uses Log4j as standard logging service, whose configuration is defined through the configurable file `docker/log4j2.properties`, passed in the `docker/start-wasp.sh` specialization (e.g. `docker/start-whitelabel-wasp.sh`).

**Note:** The default `appender.console.name` used is "Console"

## Monitoring
WASP includes two kinds of internal monitoring features that write information on the default indexed datastore (i.e. `solr` or `elastic`, default: "solr").

The resulting information can be verified and queried through the specific datastore Web UI (i.e. `banana` or `kibana`).

In order to use these monitoring features there are two options:
 - the configuration `systempipegraphs.start` and `systemproducers.start` within `docker/docker-environment.conf` have to be enabled (default: true)
 - the related System pipegraphs and producers (i.e. `LoggerPipegraph` + `LoggerProducer` and `TelemetryPipegraph`) have to be started manually

---

1. **Logger**
    - Stores application logs to the index `logger_solr_index` or `logger_elastic_index`
    - Document example:
    
      ```json
      {
        "id": "5324fc92-4867-4769-b6a0-dec2e6c47a2e",
        "log_source": "it.agilelab.bigdata.wasp.core.WaspSystem$",
        "log_level": "Info",
        "message": "The system is connected with the Zookeeper cluster of Kafka",
        "timestamp": "2018-04-16T12:40:24.205Z",
        "thread": "main",
        "cause": "",
        "stack_trace": "",
        "_version_": 1597906560371130400
      }
      ```
            
2. **Telemetry**

   Stores some telemetry metrics (generic and latency) to the index `telemetry_solr_index` or `telemetry_elastic_index`
    
    - Generic metrics
    
        Telemetry is extracted during the Monitoring phase of a pipegraph. The extracted telemetry is composed of:
        
        - `inputRows`
        - `inputRowsPerSecond`
        - `processedRowsPerSecond`
        - `durationMS` of various spark streaming events
        
        ```json
        {
            "messageId": "22cc1baf-b28e-4385-995d-c7db4e040f5c",
            "timestamp": "2018-04-09T16:54:10.652Z",
            "sourceId": "pipegraph_Telemetry Pipegraph_structuredstreaming_write_on_index_writer_telemetry_elastic_index",
            "metric": "triggerExecution-durationMs",
            "value": 1
        }
        ```
        
        `messageId` is relative to the collection of the telemetry and is a random identifier.
        
        `sourceId` is the name of the streaming query
        
    - Latency metrics
    
        Latency data is extracted per message with a subsampling applied per partition.
        
        The default sampling is one every 100 messages for partition.
        
        **Note:** To adjust the sampling factor provide the strategy with a `Configuration` object containing the key
        ```wasp.telemetry.latency.sample-one-message-every=<number of messages>```
        
        To enable latency collection the DataFrame should have a `metadata` column and the ETL block should have a declared strategy.
        
        latency is extracted in the form:
        
        ```json
        {
            "messageId": "3729",
            "timestamp": "2018-04-09T16:49:17.349Z",
            "sourceId": "testDocumentWithMetadataProducer/test-with-metadata-console-etl-enter/test-with-metadata-console-etl-exit",
            "metric": "latencyMs",
            "value": 1
        }
        ```
        
        `messageId` is assigned by the source by generating a `metadata` column
        
        `sourceId` is the path covered by the message inside wasp
        
        ```
        testDocumentWithMetadataProducer/test-with-metadata-console-etl-enter
                           ^                              ^
                          source                        enter in streaming query
        
        the timestamp will be the one recorded at the entrance of an etl block, latencyMS will be the difference between the time of
        publish on kafka and the entrance of the message in an etl block (we will measure latency to dequeue from kafka)
        
        testDocumentWithMetadataProducer/test-with-metadata-console-etl-enter/test-with-metadata-console-etl-exit
                           ^                               ^                                       ^
                        source                        enter in streaming query               exit from streaming query
        
        the timestamp will be the one recorded at the exit of an etl block, latencyMS will be the difference between the time of
        enter and the time of exit from the etl block (we will measure latency in processing)
        ```

---

The described monitoring features can also be used as "sanity check" in order to verify whether standalone applications are correctly deployed and started.