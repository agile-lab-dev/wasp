package it.agilelab.bigdata.wasp.core

import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.JsonConverter

/**
	* Default system pipegraphs: logging & raw.
	*/

object SystemPipegraphs {
	/** Logger pipegraph & related */
	lazy val loggerTopic = LoggerTopic()
	lazy val loggerProducer = LoggerProducer()
	lazy val loggerIndex = LoggerIndex()
	lazy val loggerPipegraph = LoggerPipegraph()

	/** Raw pipegraph & related */
//	lazy val rawTopic = RawTopic()
//	lazy val rawIndex = RawIndex()
//	lazy val rawPipegraph = RawPipegraph()
}

private[wasp] object LoggerTopic {

	val topic_name = "Logger"

	def apply() = TopicModel(
		name = TopicModel.name(topic_name),
		creationTime = System.currentTimeMillis,
		partitions = 3,
		replicas = 1,
		topicDataType = "avro",
		partitionKeyField = None,
		schema = JsonConverter.fromString(topicSchema).getOrElse(org.mongodb.scala.bson.BsonDocument())
	)

	private def topicSchema = s"${TopicModel.generateField("Logging", "Logging", None)}"

}

private[wasp] object LoggerProducer {

	def apply() = ProducerModel(
		name = "LoggerProducer",
		className = "it.agilelab.bigdata.wasp.producers.InternalLogProducerGuardian",
		topicName = Some(SystemPipegraphs.loggerTopic.name),
		isActive = false,
		None,
		isRemote = false,
		isSystem = true)
}

private[wasp] object LoggerIndex {

	val index_name = "Logger"

	def apply() = IndexModel(
		name = IndexModel.normalizeName(index_name),
		creationTime = System.currentTimeMillis,
		schema = JsonConverter.fromString(indexSchema),
		rollingIndex = true
	)

	private def indexSchema = s"""
    {"log":
        {"properties":{
          "log_source":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
          "log_level":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
          "log_class":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
          "message":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
          "cause":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
          "stack_trace":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"}
        }}}
		"""
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
		legacyStreamingComponents = List(
			LegacyStreamingETLModel(
				name = "write on index",
				inputs = List(ReaderModel.kafkaReader(loggerTopic.name, loggerTopic.name)),
				output = WriterModel.elasticWriter(loggerIndex.name, loggerIndex.name),
				mlModels = List(),
				strategy = None,
				kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED
			)
		),
		structuredStreamingComponents = List.empty,
		rtComponents = Nil,
		dashboard = None,
		isActive = false)
}

/*
private[wasp] object RawTopic {

	val topic_name = "Raw"

	def apply() = TopicModel(
		name = TopicModel.name(topic_name),
		creationTime = System.currentTimeMillis,
		partitions = 3,
		replicas = 1,
		topicDataType = "avro",
		partitionKeyField = None,
		schema = JsonConverter.fromString(topicSchema).getOrElse(org.mongodb.scala.bson.BsonDocument())
	)

	private def topicSchema = s"""
    {"type":"record",
    "namespace":"Raw",
    "name":"Raw",
    "fields":[
      ${TopicModel.schema_base}
    ]}"""
}

private[wasp] object RawIndex {

	val index_name = "Raw"

	def apply() = IndexModel(
		name = IndexModel.normalizeName(index_name),
		creationTime = System.currentTimeMillis,
		schema = JsonConverter.fromString(indexSchema),
		rollingIndex = true
	)

	private val indexSchema = s"""
    {"raw":
        {"properties":{
          ${IndexModel.schema_base_elastic}
        }}}"""
}

private[wasp] object RawPipegraph {
	import SystemPipegraphs._

	val rawPipegraphName = "RawPipegraph"

	def apply() = PipegraphModel(
		name = rawPipegraphName,
		description = "System Raw Pipegraph",
		owner = "system",
		isSystem = true,
		creationTime = System.currentTimeMillis,
		legacyStreamingComponents = List(
			LegacyStreamingETLModel(
				name = "write on index",
				inputs = List(
					ReaderModel.kafkaReader(
						rawTopic.name,
						rawTopic.name
					)
				),
				output = WriterModel.elasticWriter(rawIndex.name, rawIndex.name),
				mlModels = List(),
				strategy = None,
				kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED)
		),
		structuredStreamingComponents = List.empty,
		rtComponents = Nil,
		dashboard = None,
		isActive = false
	)
}
*/