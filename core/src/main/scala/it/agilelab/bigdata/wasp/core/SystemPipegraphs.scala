package it.agilelab.bigdata.wasp.core

import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, JsonConverter}
import org.json4s.JObject

/**
	* Default system pipegraphs.
	*/

object SystemPipegraphs {

	/** Logger  */

	/* Topic, Index, Raw for Producer, Pipegraph */
	lazy val loggerTopic = LoggerTopicModel()
	lazy val solrLoggerIndex = SolrLoggerIndex()
  lazy val elasticLoggerIndex = ElasticLoggerIndexModel()

	/* Producer */
	lazy val loggerProducer = LoggerProducer()

	/* Pipegraph */
	lazy val loggerPipegraph = LoggerPipegraph()




	/** Telemetry  */
	lazy val telemetryTopic = TelemetryTopicModel()
	lazy val solrTelemetryIndex = SolrTelemetryIndexModel()
  lazy val elasticTelemetryIndex = ElasticLatencyIndexModel()
  lazy val telemetryPipegraph = TelemetryPipegraph()
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

private[wasp] object TelemetryTopicModel {

	private val topic_name = "telemetry"

	def apply() = TopicModel(
		name = TopicModel.name(topic_name),
		creationTime = System.currentTimeMillis,
		partitions = 3,
		replicas = 1,
		topicDataType = "json",
		partitionKeyField = None,
		schema = JsonConverter.fromString(topicSchema).getOrElse(org.mongodb.scala.bson.BsonDocument())
	)

	private val topicSchema =
		TopicModel.generateField("telemetry", "telemetry", Some(
			"""
				|        {
				|            "name": "messageId",
				|            "type": "string",
				|            "doc": "Unique id of message whose latency was recorded"
				|        },
				|				 {
				|            "name": "timestamp",
				|            "type": "string",
				|            "doc": "Logged message timestamp in  ISO-8601 format"
				|        },
				|        {
				|            "name": "sourceId",
				|            "type": "string",
				|            "doc": "Id of the block that generated this message"
				|        },
				|        {
				|            "name": "metric",
				|            "type": "string",
				|            "doc": "Name of the metric"
				|        },
				|        {
				|            "name": "value",
				|            "type": "double",
				|            "doc": "Value of the metric"
				|        }""".stripMargin))
}

private[wasp] object SolrTelemetryIndexModel {

	val index_name = "telemetry_solr"

	import IndexModelBuilder._

	def apply(): IndexModel = IndexModelBuilder.forSolr
		.named(index_name)
		.config(Solr.Config(shards = 1,
											  replica = 1))
		.schema(Solr.Schema(
      Solr.Field("messageId", Solr.Type.String),
      Solr.Field("timestamp", Solr.Type.TrieDate),
      Solr.Field("sourceId", Solr.Type.String),
			Solr.Field("metric", Solr.Type.String),
			Solr.Field("value", Solr.Type.TrieDouble)))
		.build

}

private[wasp] object ElasticLatencyIndexModel {
  import org.json4s._

  import org.json4s.native.JsonMethods._
  import org.json4s.JsonDSL._

  import IndexModelBuilder._

  val index_name = "telemetry_elastic"

  import IndexModelBuilder._

  def apply(): IndexModel = IndexModelBuilder.forElastic
    .named(index_name)
    .config(Elastic.Config(shards = 1,
                           replica = 1))
    .schema(Elastic.Schema(indexElasticSchema))
    .build

  //noinspection ScalaUnnecessaryParentheses
  private lazy val indexElasticSchema = parse("""
        {
          "properties": {
            "messageId": {
              "type": "keyword"
            },
            "sourceId": {
              "type": "keyword"
            },
						"metric": {
               "type": "keyword"
             },
            "value": {
              "type": "double"
            },
            "timestamp": {
              "type": "date"
            }
          }
        }""").asInstanceOf[JObject]
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

private[wasp] object SolrLoggerIndex {

	val index_name = "logger_solr"

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

private[wasp] object ElasticLoggerIndexModel {
  import org.json4s._

  import org.json4s.native.JsonMethods._
  import org.json4s.JsonDSL._

  import IndexModelBuilder._

  val index_name = "logger_elastic"

  import IndexModelBuilder._

  def apply(): IndexModel = IndexModelBuilder.forElastic
    .named(index_name)
    .config(Elastic.Config(shards = 1,
                           replica = 1))
    .schema(Elastic.Schema(indexElasticSchema))
    .build

  //noinspection ScalaUnnecessaryParentheses
  private lazy val indexElasticSchema = parse(
    """
        {
          "properties": {
            "log_source": {
              "type": "keyword"
            },
            "log_level": {
              "type": "keyword"
            },
            "message": {
               "type": "text"
             },
            "timestamp": {
              "type": "date"
            },
            "thread": {
              "type": "keyword"
            },
            "cause": {
              "type": "text"
            },
            "stack_trace": {
              "type": "text"
            }
          }
        }""").asInstanceOf[JObject]
}

private[wasp] object LoggerPipegraph {
	import SystemPipegraphs._

	val loggerPipegraphName = "LoggerPipegraph"

  private def writer: WriterModel = ConfigManager.getWaspConfig.defaultIndexedDatastore match {
    case "elastic" => WriterModel.elasticWriter(
      elasticLoggerIndex.name,
      elasticLoggerIndex.name
    )
    case "solr" => WriterModel.solrWriter(
      solrLoggerIndex.name,
      solrLoggerIndex.name
    )
  }

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
        output = writer,
        mlModels = List(),
        strategy = None,
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
        config = Map())
    ),

		rtComponents = List(),
		dashboard = None)
}



private[wasp] object TelemetryPipegraph {
	import SystemPipegraphs._

	val telemetryPipegraphName = "TelemetryPipegraph"

	private def writer: WriterModel = ConfigManager.getWaspConfig.defaultIndexedDatastore match {
		case "elastic" => WriterModel.elasticWriter(
			elasticTelemetryIndex.name,
			elasticTelemetryIndex.name
		)
		case "solr" => WriterModel.solrWriter(
			solrTelemetryIndex.name,
			solrTelemetryIndex.name
		)
	}

	def apply() = PipegraphModel(
		name = telemetryPipegraphName,
		description = "System Telemetry Pipegraph",
		owner = "system",
		isSystem = true,
		creationTime = System.currentTimeMillis,

		legacyStreamingComponents = List(),
		structuredStreamingComponents = List(
			StructuredStreamingETLModel(
				name = "write on index",
				inputs = List(ReaderModel.kafkaReader(telemetryTopic.name, telemetryTopic.name)),
				output = writer,
				mlModels = List(),
				strategy = None,
				kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_RECEIVED_BASED,
				config = Map())
		),

		rtComponents = List(),
		dashboard = None)
}
