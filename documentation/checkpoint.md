## Checkpoint 

Folders automatically created:
- `/metadata`
- `/sources`
- `/offsets`
- `/commits`
- `/state`
    - created only with stateful transformations in Spark StructuredStreaming (e.g. `flatMapGroupsWithState`)

### offsets

A file for each micro-batch:

    v1
    {"batchWatermarkMs":0,"batchTimestampMs":1522145520473,"conf":{"spark.sql.shuffle.partitions":"200"}}
    {"testcheckpoint_json.topic":{"2":368,"1":395,"0":514}}

(see
[OffsetSeq version 2.2.1](https://github.com/apache/spark/blob/v2.2.1/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/OffsetSeq.scala),
[OffsetSeqMetadata](https://github.com/jaceklaskowski/spark-structured-streaming-book/blob/master/spark-sql-streaming-OffsetSeqMetadata.adoc))

- `batchWatermarkMs` - The current eventTime watermark, used to bound the lateness of data that will processed. Time unit: milliseconds

- `batchTimestampMs` - The current batch processing timestamp. Time unit: milliseconds

- `conf` - Additional conf_s to be persisted across batches, i.e. `spark.sql.shuffle.partitions` and `spark.sql.streaming.stateStore.providerClass` (implicit, not really present) Spark properties

  - `spark.sql.shuffle.partitions` (default: `200`)
  
    The number of partitions to use when shuffling data for joins.
 
    Note: It is restored from checkpoint, it will only change if you delete the checkpointed data and restart it with a "clean slate". This makes sense, because if you have checkpointed data, Spark needs to know from how many partition directories it needs to restore the previous state.
   
  - `spark.sql.streaming.stateStore.providerClass` (default: [org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider](https://github.com/apache/spark/blob/v2.2.1/sql/core/src/main/scala/org/apache/spark/sql/execution/streaming/state/HDFSBackedStateStoreProvider.scala))
    
    The fully-qualified class name to manage state data in stateful streaming queries.
    
    Note: It must be a subclass of StateStoreProvider, and must have a zero-arg constructor.

### commits

A file for each micro-batch, related to `offsets`:

    v1
    {}
    
### state
(see [IncrementalExecution](https://github.com/jaceklaskowski/spark-structured-streaming-book/blob/master/spark-sql-streaming-IncrementalExecution.adoc))

A folder for each stateful transformation (`0`, `1`, ...), each one containing a set of sub-folders (`0` to `<number_shuffle_partitions>`), each one containing:

- N files `.delta` + related `.delta.crc`

- N files `.snapshot` + related `.snapshot.crc`