package it.agilelab.bigdata.wasp.consumers.spark.http

import it.agilelab.bigdata.wasp.models.{StrategyModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}

package object etl {
  lazy val sampleStreamingETL = StructuredStreamingETLModel(
    name = "sample-data-to-console",
    streamingInput = StreamingReaderModel.kafkaReader("read-from-kafka", topic.fromKafkaInputTopic, None),
    staticInputs = List.empty,
    streamingOutput = WriterModel.kafkaWriter("write-to-kafka", topic.sampleDataOutputTopic),
    mlModels = List.empty,
    strategy = Some(StrategyModel("it.agilelab.bigdata.wasp.consumers.spark.utils.enrichment.etl.CustomEnrichmentStrategy")),
    triggerIntervalMs = Some(1000)
  )
}
