package it.agilelab.bigdata.wasp.whitelabel.models.example.iot

import it.agilelab.bigdata.wasp.models.{PipegraphModel, StrategyModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}

private[wasp] object IoTIndustrialPlantPipegraphModel {

  lazy val pipegraph = PipegraphModel(
    name = "Industrial Plant",
    description = "Plant analysis",
    owner = "user",
    isSystem = false,
    creationTime = System.currentTimeMillis,

    legacyStreamingComponents = List.empty,
    structuredStreamingComponents = List(
      StructuredStreamingETLModel(
	      name = "Write on console",
	      streamingInput = StreamingReaderModel.kafkaReader(
			      name = "Read from plant topic",
			      topicModel = IoTIndustrialPlantTopicModel.industrialPlantTopicModel,
			      rateLimit = None
		      ),
	      staticInputs = List.empty,
	      streamingOutput =  writer, //WriterModel.consoleWriter("console-writer"),
	      mlModels = List.empty,
        strategy = Some(
          StrategyModel(
            className = "it.agilelab.bigdata.wasp.consumers.spark.strategies.DropKafkaMetadata"
          )
        ),
	      triggerIntervalMs = None,
	      options = Map()
      )
    ),
    rtComponents = List(),

    dashboard = None)


  private def writer: WriterModel = WriterModel.solrWriter("Write IoT Industrial Plant data to Solr", IoTIndustrialPlantIndexModel())
}