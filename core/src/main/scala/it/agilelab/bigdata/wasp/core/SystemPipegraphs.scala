package it.agilelab.bigdata.wasp.core

import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.JsonConverter

/**
	* Default system pipegraphs.
	*/

object SystemPipegraphs {

	/** Logger  */

	/* Topic, Index, Raw for Producer, Pipegraph */
	lazy val loggerTopic = LoggerTopicModel()
	lazy val loggerIndex = LoggerIndex()

	/* Producer */
	lazy val loggerProducer = LoggerProducer()

	/* Pipegraph */
	lazy val loggerPipegraph = LoggerPipegraph()
}

private[wasp] object LoggerTopicModel {

	private val topic_name = "logger"

	def apply() = TopicModel(
		name = TopicModel.name(topic_name),
		creationTime = System.currentTimeMillis,
		partitions = 3,
		replicas = 1,
		topicDataType = "avro",
		partitionKeyField = None,
		schema = JsonConverter.fromString(topicSchema).getOrElse(org.mongodb.scala.bson.BsonDocument())
	)

	private val topicSchema =
		TopicModel.generateField("logging", "logging", Some(
			"""
				|				 {
				|            "name": "log_source",
				|            "type": "string",
				|            "doc": "Class that logged this message"
				|        },
				|        {
				|            "name": "log_level",
				|            "type": "string",
				|            "doc": "Logged message level"
				|        },
				|        {
				|            "name": "message",
				|            "type": "string",
				|            "doc": "Logged message"
				|        },
				|        {
				|            "name": "timestamp",
				|            "type": "string",
				|            "doc": "Logged message timestamp in  ISO-8601 format"
				|        },
				|        {
				|            "name": "thread",
				|            "type": "string",
				|            "doc": "Thread that logged this message"
				|        },
				|        {
				|            "name": "cause",
				|            "type": "string",
				|            "doc": "Message of the logged exception attached to this logged message",
				|        },
				|        {
				|            "name": "stack_trace",
				|            "type": "string",
				|            "doc": "Stacktrace of the logged exception attached to this logged message",
				|        },
			""".stripMargin))
}

private[wasp] object LoggerProducer {

	def apply() = ProducerModel(
		name = "LoggerProducer",
		className = "it.agilelab.bigdata.wasp.producers.InternalLogProducerGuardian",
		topicName = Some(SystemPipegraphs.loggerTopic.name),
		isActive = false,
    configuration = None,
		isRemote = false,
		isSystem = true)
}

private[wasp] object LoggerIndex {

	val index_name = "logger"

	import IndexModelBuilder._

	def apply(): IndexModel = IndexModelBuilder.forSolr
		                             						 .named(index_name)
																						 .config(Solr.Config(shards = 1,
																							 									 replica = 1))
	  																				 .schema(Solr.Schema(
																							 				Solr.Field("log_source", Solr.Type.String),
																							 				Solr.Field("log_level", Solr.Type.String),
																							 				Solr.Field("message", Solr.Type.String),
																							 				Solr.Field("timestamp", Solr.Type.TrieDate),
																							 				Solr.Field("thread", Solr.Type.String),
																							 				Solr.Field("cause", Solr.Type.String),
																							 				Solr.Field("stack_trace", Solr.Type.String)))
																 						 .build

}

private[wasp] object LoggerPipegraph {
	import SystemPipegraphs._

	val loggerPipegraphName = "LoggerPipegraph"

	def apply() = PipegraphModel(
		name = loggerPipegraphName,
		description = "System Logger Pipegraph",
		owner = "system",
		isSystem = true,
		creationTime = System.currentTimeMillis,

		legacyStreamingComponents = List(),
		structuredStreamingComponents = List(
      StructuredStreamingETLModel(
        name = "write on index",
        inputs = List(ReaderModel.kafkaReader(loggerTopic.name, loggerTopic.name)),
        output = WriterModel.solrWriter(
          loggerIndex.name,
          loggerIndex.name
        ),
        mlModels = List(),
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map())
    ),

		rtComponents = List(),
		dashboard = None)
}