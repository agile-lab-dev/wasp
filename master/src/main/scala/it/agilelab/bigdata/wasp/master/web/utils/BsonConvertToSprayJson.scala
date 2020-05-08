package it.agilelab.bigdata.wasp.master.web.utils

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.{TemporalAccessor, TemporalQuery}

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.typesafe.config._
import it.agilelab.bigdata.wasp.core.datastores.{DatastoreProduct, TopicCategory}
import it.agilelab.bigdata.wasp.core.models.{LogEntry, _}
import it.agilelab.bigdata.wasp.core.models.configuration._
import it.agilelab.bigdata.wasp.core.utils.{ConnectionConfig, DatastoreProductJsonFormat, ZookeeperConnectionsConfig}
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}
import spray.json.{JsValue, RootJsonFormat, _}

/**
  * Created by Agile Lab s.r.l. on 04/08/2017.
  */
object BsonConvertToSprayJson extends SprayJsonSupport with DefaultJsonProtocol {

  implicit object JsonFormatDocument extends RootJsonFormat[BsonDocument] {
    def write(c: BsonDocument): JsValue = c.toJson.parseJson

    def read(value: JsValue): BsonDocument = BsonDocument(value.toString())
  }

  implicit object JsonFormatObjectId extends RootJsonFormat[BsonObjectId] {
    def write(c: BsonObjectId): JsValue = c.getValue.toHexString.toJson

    def read(value: JsValue): BsonObjectId = value match {
      case JsString(objectId) => BsonObjectId(objectId)
      case _ => deserializationError("String expected")
    }
  }

}


/**
  * Based on the code found: https://groups.google.com/forum/#!topic/spray-user/RkIwRIXzDDc
  */
class EnumJsonConverter[T <: scala.Enumeration](enu: T) extends RootJsonFormat[T#Value] {

  import spray.json.{DeserializationException, JsString, JsValue}

  override def write(obj: T#Value): JsValue = JsString(obj.toString)

  override def read(json: JsValue): T#Value = json match {
    case JsString(txt) => enu.withName(txt)
    case somethingElse => throw DeserializationException(s"Expected a value from enum $enu instead of $somethingElse")
  }
}

class TypesafeConfigJsonConverter() extends RootJsonFormat[Config] {
  val ParseOptions = ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON)
  val RenderOptions = ConfigRenderOptions.concise().setJson(true)

  override def write(config: Config): JsValue = JsonParser(config.root.render(RenderOptions))

  override def read(jsValue: JsValue): Config = jsValue match {
    case obj: JsObject => ConfigFactory.parseString(obj.compactPrint, ParseOptions)
    case _ => deserializationError("Expected JsObject for Config deserialization")
  }
}

/**
  * RootJsonFormat for topic datastore models.
  *
  * @author Nicolò Bidotti
  */
class TopicDatastoreModelJsonFormat
  extends RootJsonFormat[DatastoreModel[TopicCategory]]
    with SprayJsonSupport
    with DefaultJsonProtocol {

  import it.agilelab.bigdata.wasp.master.web.utils.BsonConvertToSprayJson._

  implicit lazy val topicCompressionFormat: JsonFormat[TopicCompression] = new JsonFormat[TopicCompression] {
    override def write(obj: TopicCompression): JsValue =
      JsString(TopicCompression.asString(obj))

    override def read(json: JsValue): TopicCompression =
      json match {
        case JsString(value) => TopicCompression.fromString(value)
      }
  }

  implicit val topicModelFormat: RootJsonFormat[TopicModel] = jsonFormat11(TopicModel.apply)
  implicit val multiTopicModelFormat: RootJsonFormat[MultiTopicModel] = jsonFormat3(MultiTopicModel.apply)

  override def write(obj: DatastoreModel[TopicCategory]): JsValue = {
    obj match {
      case topicModel: TopicModel => topicModel.toJson
      case multiTopicModel: MultiTopicModel => multiTopicModel.toJson
    }
  }

  override def read(json: JsValue): DatastoreModel[TopicCategory] = {
    val obj = json.asJsObject
    val fields = obj.fields
    obj match {
      case topicModel if fields.contains("partitions") => topicModel.convertTo[TopicModel]
      case multiTopicModel if fields.contains("topicNameField") => multiTopicModel.convertTo[MultiTopicModel]
    }
  }
}


// collect your json format instances into a support trait:
trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol with DataStoreConfJsonSupport with BatchJobJsonSupport {

  import it.agilelab.bigdata.wasp.master.web.utils.BsonConvertToSprayJson._

  implicit lazy val instant: RootJsonFormat[Instant] = new RootJsonFormat[Instant] {
    override def write(obj: Instant): JsValue = JsString(DateTimeFormatter.ISO_INSTANT.format(obj))

    override def read(json: JsValue): Instant = json match {
      case JsString(value) => DateTimeFormatter.ISO_INSTANT.parse(value, new TemporalQuery[Instant] {
        override def queryFrom(temporal: TemporalAccessor): Instant = Instant.from(temporal)
      })
    }
  }

  implicit lazy val telemetryPointFormat: RootJsonFormat[TelemetryPoint] = jsonFormat2(TelemetryPoint.apply)
  implicit lazy val telemetrySeriesFormat: RootJsonFormat[TelemetrySeries] = jsonFormat3(TelemetrySeries.apply)
  implicit lazy val metricEntryFormat: RootJsonFormat[MetricEntry] = jsonFormat2(MetricEntry.apply)
  implicit lazy val metricsFormat: RootJsonFormat[Metrics] = jsonFormat2(Metrics.apply)
  implicit lazy val logsFormat: RootJsonFormat[Logs] = jsonFormat2(Logs.apply)
  implicit lazy val logEntryFormat: RootJsonFormat[LogEntry] = jsonFormat7(LogEntry.apply)
  implicit lazy val sourceEntryFormat: RootJsonFormat[SourceEntry] = jsonFormat1(SourceEntry.apply)
  implicit lazy val sourcesFormat: RootJsonFormat[Sources] = jsonFormat2(Sources.apply)
  implicit lazy val eventsFormat: RootJsonFormat[Events] = jsonFormat2(Events.apply)
  implicit lazy val eventEntryFormat: RootJsonFormat[EventEntry] = jsonFormat8(EventEntry.apply)
  implicit lazy val jmxTelemetryTopicConfigModel: RootJsonFormat[JMXTelemetryConfigModel] = jsonFormat5(JMXTelemetryConfigModel.apply)

  implicit lazy val telemetryTopicConfigModel: RootJsonFormat[TelemetryTopicConfigModel] = jsonFormat5(TelemetryTopicConfigModel.apply)
  implicit lazy val telemetryConfigModel: RootJsonFormat[TelemetryConfigModel] = jsonFormat4(TelemetryConfigModel.apply)

  implicit lazy val topicDatastoreModel: RootJsonFormat[DatastoreModel[TopicCategory]] = new TopicDatastoreModelJsonFormat
  implicit lazy val indexModelFormat: RootJsonFormat[IndexModel] = jsonFormat8(IndexModel.apply)
  implicit lazy val datastoreProductFormat: RootJsonFormat[DatastoreProduct] = DatastoreProductJsonFormat
  implicit lazy val streamingReaderModelFormat: RootJsonFormat[StreamingReaderModel] = jsonFormat5(
    (name: String,
     datastoreModelName: String,
     datastoreProduct: DatastoreProduct,
     rateLimit: Option[Int],
     options: Map[String, String]) =>
      StreamingReaderModel(name, datastoreModelName, datastoreProduct, rateLimit, options))
  implicit lazy val readerModelFormat: RootJsonFormat[ReaderModel] = jsonFormat4(
    (name: String,
     datastoreModelName: String,
     datastoreProduct: DatastoreProduct,
     options: Map[String, String]) => ReaderModel(name, datastoreModelName, datastoreProduct, options))
  implicit lazy val writerModelFormat: RootJsonFormat[WriterModel] = jsonFormat4(
    (name: String,
     datastoreModelName: String,
     datastoreProduct: DatastoreProduct,
     options: Map[String, String]) => WriterModel(name, datastoreModelName, datastoreProduct, options))


  implicit lazy val mlModelOnlyInfoFormat: RootJsonFormat[MlModelOnlyInfo] = jsonFormat7(MlModelOnlyInfo.apply)
  implicit lazy val strategyModelFormat: RootJsonFormat[StrategyModel] = jsonFormat2(StrategyModel.apply)
  implicit lazy val dashboardModelFormat: RootJsonFormat[DashboardModel] = jsonFormat2(DashboardModel.apply)
  implicit lazy val etlModelFormat: RootJsonFormat[LegacyStreamingETLModel] = jsonFormat8(LegacyStreamingETLModel.apply)
  implicit lazy val etlStructuredModelFormat: RootJsonFormat[StructuredStreamingETLModel] = jsonFormat9(StructuredStreamingETLModel.apply)
  implicit lazy val rTModelFormat: RootJsonFormat[RTModel] = jsonFormat5(RTModel.apply)
  implicit lazy val pipegraphModelFormat: RootJsonFormat[PipegraphModel] = jsonFormat9(PipegraphModel.apply)
  implicit lazy val connectionConfigFormat: RootJsonFormat[ConnectionConfig] = jsonFormat5(ConnectionConfig.apply)
  implicit lazy val zookeeperConnectionFormat: RootJsonFormat[ZookeeperConnectionsConfig] = jsonFormat2(ZookeeperConnectionsConfig.apply)
  implicit lazy val kafkaEntryConfigModelFormat: RootJsonFormat[KafkaEntryConfig] = jsonFormat2(KafkaEntryConfig.apply)
  implicit lazy val kafkaConfigModelFormat: RootJsonFormat[KafkaConfigModel] = jsonFormat13(KafkaConfigModel.apply)
  implicit lazy val sparkDriverConfigFormat: RootJsonFormat[SparkDriverConfig] = jsonFormat7(SparkDriverConfig.apply)
  implicit lazy val kryoSerializerConfigFormat: RootJsonFormat[KryoSerializerConfig] = jsonFormat3(KryoSerializerConfig.apply)
  implicit lazy val sparkEntryConfigModelConfigFormat: RootJsonFormat[SparkEntryConfig] = jsonFormat2(SparkEntryConfig.apply)
  implicit lazy val sparkStreamingConfigModelFormat: RootJsonFormat[SparkStreamingConfigModel] = jsonFormat21(SparkStreamingConfigModel.apply)
  implicit lazy val sparkBatchConfigModelFormat: RootJsonFormat[SparkBatchConfigModel] = jsonFormat18(SparkBatchConfigModel.apply)
  implicit lazy val hbaseEntryConfigModelConfigFormat: RootJsonFormat[HBaseEntryConfig] = jsonFormat2(HBaseEntryConfig.apply)
  implicit lazy val hbaseConfigModelConfigFormat: RootJsonFormat[HBaseConfigModel] = jsonFormat4(HBaseConfigModel.apply)
  implicit lazy val elasticConfigModelFormat: RootJsonFormat[ElasticConfigModel] = jsonFormat2(ElasticConfigModel.apply)
  implicit lazy val solrConfigModelFormat: RootJsonFormat[SolrConfigModel] = jsonFormat2(SolrConfigModel.apply)
  implicit lazy val batchJobExclusionConfig: RootJsonFormat[BatchJobExclusionConfig] = jsonFormat2(BatchJobExclusionConfig.apply)
  implicit lazy val dataStoreConfFormat: RootJsonFormat[DataStoreConf] = createDataStoreConfFormat
  implicit lazy val batchETLModelFormat: RootJsonFormat[BatchETLModel] = jsonFormat8(BatchETLModel.apply)
  implicit lazy val batchETLGdprModelFormat: RootJsonFormat[BatchGdprETLModel] =
    jsonFormat(
      BatchGdprETLModel.apply,
      "name",
      "dataStores",
      "strategyConfig",
      "inputs",
      "output",
      "group",
      "isActive")
  implicit lazy val batchETLFormat: RootJsonFormat[BatchETL] = createBatchETLFormat
  implicit lazy val batchJobModelFormat: RootJsonFormat[BatchJobModel] = jsonFormat7(BatchJobModel.apply)
  implicit lazy val producerModelFormat: RootJsonFormat[ProducerModel] = jsonFormat7(ProducerModel.apply)
  implicit lazy val jobStatusFormat: RootJsonFormat[JobStatus.JobStatus] = new EnumJsonConverter(JobStatus)
  implicit lazy val typesafeConfigFormat: RootJsonFormat[Config] = new TypesafeConfigJsonConverter
  implicit lazy val batchJobInstanceModelFormat: RootJsonFormat[BatchJobInstanceModel] = jsonFormat7(BatchJobInstanceModel.apply)
  implicit lazy val pipegraphStatusFormat: RootJsonFormat[PipegraphStatus.PipegraphStatus] = new EnumJsonConverter(PipegraphStatus)
  implicit lazy val pipegraphInstanceModelFormat: RootJsonFormat[PipegraphInstanceModel] = jsonFormat6(PipegraphInstanceModel.apply)
  implicit lazy val documentModelFormat: RootJsonFormat[DocumentModel] = jsonFormat3(DocumentModel.apply)

}

/*trait GdprJsonSupport extends DefaultJsonProtocol {
  implicit lazy val exactKeyValueMatchingStrategyFormat: RootJsonFormat[ExactKeyValueMatchingStrategy] = jsonFormat1(ExactKeyValueMatchingStrategy.apply)
  implicit lazy val prefixKeyValueMatchingStrategyFormat: RootJsonFormat[PrefixKeyValueMatchingStrategy] = jsonFormat1(PrefixKeyValueMatchingStrategy.apply)
  implicit lazy val prefixAndTimeBoundKeyValueMatchingStrategyFormat: RootJsonFormat[PrefixAndTimeBoundKeyValueMatchingStrategy] = jsonFormat4(PrefixAndTimeBoundKeyValueMatchingStrategy.apply)
  implicit lazy val keyValueMatchingStrategyFormat: RootJsonFormat[KeyValueMatchingStrategy] = new RootJsonFormat[KeyValueMatchingStrategy] {
    override def read(json: JsValue): KeyValueMatchingStrategy = {
      json.asJsObject.getFields("type") match {
        case Seq(JsString("ExactKeyValueMatchingStrategy")) => json.convertTo[ExactKeyValueMatchingStrategy]
        case Seq(JsString("PrefixKeyValueMatchingStrategy")) => json.convertTo[PrefixKeyValueMatchingStrategy]
        case Seq(JsString("PrefixAndTimeBoundKeyValueMatchingStrategy")) => json.convertTo[PrefixAndTimeBoundKeyValueMatchingStrategy]
        case _ => throw DeserializationException("Unknown json")
      }
    }

    override def write(obj: KeyValueMatchingStrategy): JsValue = JsObject((obj match {
      case e: ExactKeyValueMatchingStrategy => e.toJson
      case p: PrefixKeyValueMatchingStrategy => p.toJson
      case pt: PrefixAndTimeBoundKeyValueMatchingStrategy => pt.toJson
    }).asJsObject.fields + ("type" -> JsString(obj.getClass.getName)))
  }

  implicit lazy val keyValueOptionFormat: RootJsonFormat[KeyValueOption] = jsonFormat2(KeyValueOption.apply)
  implicit lazy val keyValueModelFormat: RootJsonFormat[KeyValueModel] = jsonFormat6(KeyValueModel.apply)
  implicit lazy val keyValueDataStoreConfFormat: RootJsonFormat[KeyValueDataStoreConf] = jsonFormat2(KeyValueDataStoreConf.apply)

  implicit lazy val timeBasedBetweenPartitionPruningStrategyFormat: RootJsonFormat[TimeBasedBetweenPartitionPruningStrategy] =
    jsonFormat3(TimeBasedBetweenPartitionPruningStrategy)
  implicit lazy val partitionPruningFormat: RootJsonFormat[PartitionPruningStrategy] = new RootJsonFormat[PartitionPruningStrategy] {
    override def read(json: JsValue): PartitionPruningStrategy = json.asJsObject.getFields("type") match {
      case Seq(JsString("TimeBasedBetweenPartitionPruningStrategy")) => json.convertTo[TimeBasedBetweenPartitionPruningStrategy]
      case Seq(JsString("NoPartitionPruningStrategy")) => NoPartitionPruningStrategy
      case _ => throw DeserializationException("Unknown json")
    }

    override def write(obj: PartitionPruningStrategy): JsValue = JsObject((obj match {
      case timeBased: TimeBasedBetweenPartitionPruningStrategy => timeBased.toJson
      case NoPartitionPruningStrategy => JsObject()
    }).asJsObject.fields + ("type" -> JsString(obj.getClass.getName)))
  }

  implicit lazy val exactRawMatchingStrategyFormat: RootJsonFormat[ExactRawMatchingStrategy] = jsonFormat2(ExactRawMatchingStrategy)
  implicit lazy val prefixRawMatchingStrategyFormat: RootJsonFormat[PrefixRawMatchingStrategy] = jsonFormat2(PrefixRawMatchingStrategy)
  implicit lazy val rawMatchingStrategyFormat: RootJsonFormat[RawMatchingStrategy] = new RootJsonFormat[RawMatchingStrategy] {
    override def read(json: JsValue): RawMatchingStrategy = json.asJsObject.getFields("type") match {
      case Seq(JsString("ExactRawMatchingStrategy")) => json.convertTo[ExactRawMatchingStrategy]
      case Seq(JsString("PrefixRawMatchingStrategy")) => json.convertTo[PrefixRawMatchingStrategy]
      case _ => throw DeserializationException("Unknown json")
    }

    override def write(obj: RawMatchingStrategy): JsValue = JsObject((obj match {
      case exact: ExactRawMatchingStrategy => exact.toJson
      case prefix: PrefixRawMatchingStrategy => prefix.toJson
    }).asJsObject.fields + ("type" -> JsString(obj.getClass.getName)))
  }
//  implicit lazy val rawOptionsFormat: RootJsonFormat[RawOptions] = jsonFormat4(RawOptions.apply)
//  implicit lazy val rawModelFormat: RootJsonFormat[RawModel] = jsonFormat5(RawModel.apply)
  implicit lazy val rawDataStoreConfFormat: RootJsonFormat[RawDataStoreConf] = jsonFormat3(RawDataStoreConf)
  implicit lazy val dataStoreConfFormat: RootJsonFormat[DataStoreConf] = new RootJsonFormat[DataStoreConf] {
    override def read(json: JsValue): DataStoreConf = json.asJsObject.getFields("type") match {
      case Seq(JsString("KeyValueDataStoreConf")) => json.convertTo[KeyValueDataStoreConf]
      case Seq(JsString("RawDataStoreConf")) => json.convertTo[RawDataStoreConf]
      case _ => throw DeserializationException("Unknown json")
    }

    override def write(obj: DataStoreConf): JsValue = JsObject((obj match {
      case kv: KeyValueDataStoreConf => kv.toJson
      case raw: RawDataStoreConf => raw.toJson
    }).asJsObject.fields + ("type" -> JsString(obj.getClass.getName)))
  }

}*/
