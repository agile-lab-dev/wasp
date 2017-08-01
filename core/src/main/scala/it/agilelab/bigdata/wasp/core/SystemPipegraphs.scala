package it.agilelab.bigdata.wasp.core

import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.ReaderModel._
import it.agilelab.bigdata.wasp.core.models.WriterModel._
import it.agilelab.bigdata.wasp.core.utils.BSONFormats
import reactivemongo.bson.BSONObjectID

/**
	* Default system pipegraphs: logging & raw.
	*/
object SystemPipegraphs {
	/** Logger pipegraph & related */
	lazy val loggerPipegraphName = "LoggerPipegraph"
	lazy val loggerTopic = LoggerTopic()
	lazy val loggerProducer = LoggerProducer()
	lazy val loggerIndex = LoggerIndex()
	lazy val loggerPipegraph = LoggerPipegraph()

	/** Raw pipegraph & related */
	lazy val rawPipegraphName = "RawPipegraph"
	lazy val rawTopic = RawTopic()
	lazy val rawIndex = RawIndex()
	lazy val rawPipegraph = RawPipegraph()
}

private[wasp] object LoggerTopic {

	val topic_name = "Logger"

	def apply() = TopicModel(
		name = TopicModel.name(topic_name),
		creationTime = WaspSystem.now,
		partitions = 3,
		replicas = 1,
		topicDataType = "avro",
		schema = Some(BSONFormats.fromString(topicSchema).get),
		_id = Some(BSONObjectID.generate)
	)

	private def topicSchema = s"""
    {"type":"record",
    "namespace":"Logging",
    "name":"Logging",
    "fields":[
      ${TopicModel.schema_base},
      {"name":"log_source","type":"string"},
      {"name":"log_class","type":"string"},
      {"name":"log_level","type":"string"},
      {"name":"message","type":"string"},
      {"name":"cause","type":"string"},
      {"name":"stack_trace","type":"string"}
    ]}"""
}

private[wasp] object LoggerProducer {
	import SystemPipegraphs._

	def apply() = ProducerModel(
		name = "LoggerProducer",
		className = "it.agilelab.bigdata.wasp.producers.InternalLogProducerGuardian",
		id_topic = Some(loggerTopic._id.get),
		isActive = false,
		None,
		isRemote = false,
		Some(BSONObjectID.generate)
	)
}

private[wasp] object LoggerIndex {

	val index_name = "Logger"

	def apply() = IndexModel(
		name = IndexModel.normalizeName(index_name),
		creationTime = WaspSystem.now,
		schema = BSONFormats.fromString(indexSchema),
		_id = Some(BSONObjectID.generate)
	)

	private def indexSchema = s"""
    {"log":
        {"properties":{
          ${IndexModel.schema_base_elastic},
          "log_source":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
          "log_level":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
          "log_class":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
          "message":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
          "cause":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"},
          "stack_trace":{"type":"string","index":"not_analyzed","store":"true","enabled":"true"}
        }}}"""
}

private[wasp] object LoggerPipegraph {
	import SystemPipegraphs._

	def apply() = PipegraphModel(
		name = SystemPipegraphs.loggerPipegraphName,
		description = "System Logger Pipegraph",
		owner = "system",
		system = true,
		creationTime = WaspSystem.now,
		etl = List(ETLModel(
			"write on index", List(TopicReader(loggerTopic._id.get, loggerTopic.name)),
			IndexWriter(loggerIndex._id.get, loggerIndex.name), List(),None, ETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED)
		),
		rt = Nil,
		dashboard = None,
		isActive = true,
		_id = Some(BSONObjectID.generate)
	)
}

private[wasp] object RawTopic {

	val topic_name = "Raw"

	def apply() = TopicModel(
		name = TopicModel.name(topic_name),
		creationTime = WaspSystem.now,
		partitions = 3,
		replicas = 1,
		topicDataType = "avro",
		schema = Some(BSONFormats.fromString(topicSchema).get),
		_id = Some(BSONObjectID.generate)
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
		creationTime = WaspSystem.now,
		schema = BSONFormats.fromString(indexSchema),
		_id = Some(BSONObjectID.generate)
	)

	private val indexSchema = s"""
    {"raw":
        {"properties":{
          ${IndexModel.schema_base_elastic}
        }}}"""
}

private[wasp] object RawPipegraph {
	import SystemPipegraphs._

	def apply() = PipegraphModel(
		name = SystemPipegraphs.rawPipegraphName,
		description = "System Raw Pipegraph",
		owner = "system",
		system = true,
		creationTime = WaspSystem.now,
		etl = List(ETLModel(
			"write on index",
			List(TopicReader(rawTopic._id.get, rawTopic.name)),
			IndexWriter(rawIndex._id.get, rawIndex.name), List(), None, ETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED)
		),
		rt = Nil,
		dashboard = None,
		isActive = true,
		_id = Some(BSONObjectID.generate)
	)
}
