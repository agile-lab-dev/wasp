package it.agilelab.bigdata.wasp.utils

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.{TemporalAccessor, TemporalQuery}

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.typesafe.config._
import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct, TopicCategory}
import it.agilelab.bigdata.wasp.models.configuration._
import it.agilelab.bigdata.wasp.models.editor._
import it.agilelab.bigdata.wasp.models._
import org.json4s.{DefaultFormats, Formats, JObject}
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}
import spray.json.{JsValue, RootJsonFormat, deserializationError, _}

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
      case _                  => deserializationError("String expected")
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
  val ParseOptions  = ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON)
  val RenderOptions = ConfigRenderOptions.concise().setJson(true)

  override def write(config: Config): JsValue = JsonParser(config.root.render(RenderOptions))

  override def read(jsValue: JsValue): Config = jsValue match {
    case obj: JsObject => ConfigFactory.parseString(obj.compactPrint, ParseOptions)
    case _             => deserializationError("Expected JsObject for Config deserialization")
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

  import BsonConvertToSprayJson._

  implicit lazy val topicCompressionFormat: JsonFormat[TopicCompression] = new JsonFormat[TopicCompression] {
    override def write(obj: TopicCompression): JsValue =
      JsString(TopicCompression.asString(obj))

    override def read(json: JsValue): TopicCompression =
      json match {
        case JsString(value) => TopicCompression.fromString(value)
      }
  }

  implicit val topicModelFormat: RootJsonFormat[TopicModel]           = jsonFormat11(TopicModel.apply)
  implicit val multiTopicModelFormat: RootJsonFormat[MultiTopicModel] = jsonFormat3(MultiTopicModel.apply)

  override def write(obj: DatastoreModel[TopicCategory]): JsValue = {
    obj match {
      case topicModel: TopicModel           => topicModel.toJson
      case multiTopicModel: MultiTopicModel => multiTopicModel.toJson
    }
  }

  override def read(json: JsValue): DatastoreModel[TopicCategory] = {
    val obj    = json.asJsObject
    val fields = obj.fields
    obj match {
      case topicModel if fields.contains("partitions")          => topicModel.convertTo[TopicModel]
      case multiTopicModel if fields.contains("topicNameField") => multiTopicModel.convertTo[MultiTopicModel]
    }
  }
}

// collect your json format instances into a support trait:
trait JsonSupport
    extends SprayJsonSupport
    with DefaultJsonProtocol
    with DataStoreConfJsonSupport
    with BatchJobJsonSupport {

  import BsonConvertToSprayJson._

  implicit lazy val instant: RootJsonFormat[Instant] = new RootJsonFormat[Instant] {
    override def write(obj: Instant): JsValue = JsString(DateTimeFormatter.ISO_INSTANT.format(obj))

    override def read(json: JsValue): Instant = json match {
      case JsString(value) =>
        DateTimeFormatter.ISO_INSTANT.parse(value, new TemporalQuery[Instant] {
          override def queryFrom(temporal: TemporalAccessor): Instant = Instant.from(temporal)
        })
    }
  }
  implicit lazy val batchSchedulerModelFormat: RootJsonFormat[BatchSchedulerModel] = jsonFormat5(BatchSchedulerModel.apply)
  implicit lazy val countEntryFormat: RootJsonFormat[CountEntry] = jsonFormat2(CountEntry.apply)
  implicit lazy val countsFormat: RootJsonFormat[Counts]         = jsonFormat3(Counts.apply)

  implicit lazy val telemetryPointFormat: RootJsonFormat[TelemetryPoint]   = jsonFormat2(TelemetryPoint.apply)
  implicit lazy val telemetrySeriesFormat: RootJsonFormat[TelemetrySeries] = jsonFormat3(TelemetrySeries.apply)
  implicit lazy val metricEntryFormat: RootJsonFormat[MetricEntry]         = jsonFormat2(MetricEntry.apply)
  implicit lazy val metricsFormat: RootJsonFormat[Metrics]                 = jsonFormat2(Metrics.apply)
  implicit lazy val logsFormat: RootJsonFormat[Logs]                       = jsonFormat2(Logs.apply)
  implicit lazy val logEntryFormat: RootJsonFormat[LogEntry]               = jsonFormat7(LogEntry.apply)
  implicit lazy val sourceEntryFormat: RootJsonFormat[SourceEntry]         = jsonFormat1(SourceEntry.apply)
  implicit lazy val sourcesFormat: RootJsonFormat[Sources]                 = jsonFormat2(Sources.apply)
  implicit lazy val eventsFormat: RootJsonFormat[Events]                   = jsonFormat2(Events.apply)
  implicit lazy val eventEntryFormat: RootJsonFormat[EventEntry]           = jsonFormat8(EventEntry.apply)
  implicit lazy val jmxTelemetryTopicConfigModel: RootJsonFormat[JMXTelemetryConfigModel] = jsonFormat5(
    JMXTelemetryConfigModel.apply
  )
  implicit lazy val jdbcConnectionConfigFormat : RootJsonFormat[JdbcConnectionConfig] = jsonFormat5(JdbcConnectionConfig.apply)
  implicit lazy val jdbcConfigModelFormat :  RootJsonFormat[JdbcConfigModel] = jsonFormat2(JdbcConfigModel.apply)
  implicit lazy val nifiConfigModelFormat :  RootJsonFormat[NifiConfigModel] = jsonFormat4(NifiConfigModel.apply)
  implicit lazy val compilerConfigModelFormat :  RootJsonFormat[CompilerConfigModel] = jsonFormat2(CompilerConfigModel.apply)


  implicit lazy val telemetryTopicConfigModel: RootJsonFormat[TelemetryTopicConfigModel] = jsonFormat5(
    TelemetryTopicConfigModel.apply
  )
  implicit lazy val telemetryConfigModel: RootJsonFormat[TelemetryConfigModel] = jsonFormat4(TelemetryConfigModel.apply)

  implicit lazy val topicDatastoreModel: RootJsonFormat[DatastoreModel[TopicCategory]] =
    new TopicDatastoreModelJsonFormat
  implicit lazy val indexModelFormat: RootJsonFormat[IndexModel]             = jsonFormat9(IndexModel.apply)
  implicit lazy val datastoreProductFormat: RootJsonFormat[DatastoreProduct] = DatastoreProductJsonFormat
  implicit lazy val streamingReaderModelFormat: RootJsonFormat[StreamingReaderModel] = jsonFormat5(
    (
        name: String,
        datastoreModelName: String,
        datastoreProduct: DatastoreProduct,
        rateLimit: Option[Int],
        options: Map[String, String]
    ) => StreamingReaderModel(name, datastoreModelName, datastoreProduct, rateLimit, options)
  )
  implicit lazy val readerModelFormat: RootJsonFormat[ReaderModel] = jsonFormat4(
    (name: String, datastoreModelName: String, datastoreProduct: DatastoreProduct, options: Map[String, String]) =>
      ReaderModel(name, datastoreModelName, datastoreProduct, options)
  )
  implicit lazy val writerModelFormat: RootJsonFormat[WriterModel] = jsonFormat4(
    (name: String, datastoreModelName: String, datastoreProduct: DatastoreProduct, options: Map[String, String]) =>
      WriterModel(name, datastoreModelName, datastoreProduct, options)
  )

  implicit lazy val rawModelFormat: RootJsonFormat[RawModel]                  = jsonFormat5(RawModel.apply)
  implicit lazy val rawOptionModelFormat: RootJsonFormat[RawOptions]          = jsonFormat4(RawOptions.apply)
  implicit lazy val keyValueModelFormat: RootJsonFormat[KeyValueModel]        = jsonFormat6(KeyValueModel.apply)
  implicit lazy val keyValueOptionModelFormat: RootJsonFormat[KeyValueOption] = jsonFormat2(KeyValueOption.apply)

  implicit lazy val mlModelOnlyInfoFormat: RootJsonFormat[MlModelOnlyInfo]  = jsonFormat7(MlModelOnlyInfo.apply)
  implicit lazy val strategyModelFormat: RootJsonFormat[StrategyModel]      = jsonFormat2(StrategyModel.apply)
  implicit lazy val dashboardModelFormat: RootJsonFormat[DashboardModel]    = jsonFormat2(DashboardModel.apply)
  implicit lazy val etlModelFormat: RootJsonFormat[LegacyStreamingETLModel] = jsonFormat8(LegacyStreamingETLModel.apply)
  implicit lazy val etlStructuredModelFormat: RootJsonFormat[StructuredStreamingETLModel] = jsonFormat9(
    StructuredStreamingETLModel.apply
  )
  implicit lazy val rTModelFormat: RootJsonFormat[RTModel]                   = jsonFormat5(RTModel.apply)
  implicit lazy val pipegraphModelFormat: RootJsonFormat[PipegraphModel]     = jsonFormat10(PipegraphModel.apply)
  implicit lazy val connectionConfigFormat: RootJsonFormat[ConnectionConfig] = jsonFormat5(ConnectionConfig.apply)
  implicit lazy val zookeeperConnectionFormat: RootJsonFormat[ZookeeperConnectionsConfig] = jsonFormat2(
    ZookeeperConnectionsConfig.apply
  )
  implicit lazy val kafkaEntryConfigModelFormat: RootJsonFormat[KafkaEntryConfig] = jsonFormat2(KafkaEntryConfig.apply)
  implicit lazy val kafkaConfigModelFormat: RootJsonFormat[KafkaConfigModel]      = jsonFormat13(KafkaConfigModel.apply)
  implicit lazy val sparkDriverConfigFormat: RootJsonFormat[SparkDriverConfig]    = jsonFormat7(SparkDriverConfig.apply)
  implicit lazy val kryoSerializerConfigFormat: RootJsonFormat[KryoSerializerConfig] = jsonFormat3(
    KryoSerializerConfig.apply
  )
  implicit lazy val sparkEntryConfigModelConfigFormat: RootJsonFormat[SparkEntryConfig] = jsonFormat2(
    SparkEntryConfig.apply
  )
  implicit lazy val nifiStatelessConfigModelFormat: RootJsonFormat[NifiStatelessConfigModel] = jsonFormat4(NifiStatelessConfigModel.apply)
  implicit lazy val retainedConfigModelFormat: RootJsonFormat[RetainedConfigModel]  = jsonFormat5(RetainedConfigModel.apply)
  implicit lazy val schedulingStrategyConfigModelFormat: RootJsonFormat[SchedulingStrategyConfigModel] = jsonFormat2(SchedulingStrategyConfigModel.apply)
  implicit lazy val sparkStreamingConfigModelFormat: RootJsonFormat[SparkStreamingConfigModel] = jsonFormat19(
    SparkStreamingConfigModel.apply
  )
  implicit lazy val sparkBatchConfigModelFormat: RootJsonFormat[SparkBatchConfigModel] = jsonFormat14(
    SparkBatchConfigModel.apply
  )
  implicit lazy val hbaseEntryConfigModelConfigFormat: RootJsonFormat[HBaseEntryConfig] = jsonFormat2(
    HBaseEntryConfig.apply
  )
  implicit lazy val hbaseConfigModelConfigFormat: RootJsonFormat[HBaseConfigModel] = jsonFormat4(HBaseConfigModel.apply)
  implicit lazy val elasticConfigModelFormat: RootJsonFormat[ElasticConfigModel]   = jsonFormat2(ElasticConfigModel.apply)
  implicit lazy val solrConfigModelFormat: RootJsonFormat[SolrConfigModel]         = jsonFormat2(SolrConfigModel.apply)
  implicit lazy val batchJobExclusionConfig: RootJsonFormat[BatchJobExclusionConfig] = jsonFormat2(
    BatchJobExclusionConfig.apply
  )
  implicit lazy val dataStoreConfFormat: RootJsonFormat[DataStoreConf] = createDataStoreConfFormat
  implicit lazy val batchETLModelFormat: RootJsonFormat[BatchETLModel] = jsonFormat8(BatchETLModel.apply)
  implicit lazy val batchETLGdprModelFormat: RootJsonFormat[BatchGdprETLModel] =
    jsonFormat(BatchGdprETLModel.apply, "name", "dataStores", "strategyConfig", "inputs", "output", "group", "isActive")
  implicit lazy val batchETLFormat: RootJsonFormat[BatchETL]             = createBatchETLFormat
  implicit lazy val batchJobModelFormat: RootJsonFormat[BatchJobModel]   = jsonFormat7(BatchJobModel.apply)
  implicit lazy val producerModelFormat: RootJsonFormat[ProducerModel]   = jsonFormat7(ProducerModel.apply)
  implicit lazy val jobStatusFormat: RootJsonFormat[JobStatus.JobStatus] = new EnumJsonConverter(JobStatus)
  implicit lazy val typesafeConfigFormat: RootJsonFormat[Config]         = new TypesafeConfigJsonConverter
  implicit lazy val batchJobInstanceModelFormat: RootJsonFormat[BatchJobInstanceModel] = jsonFormat7(
    BatchJobInstanceModel.apply
  )
  implicit lazy val pipegraphStatusFormat: RootJsonFormat[PipegraphStatus.PipegraphStatus] = new EnumJsonConverter(
    PipegraphStatus
  )
  implicit lazy val pipegraphInstanceModelFormat: RootJsonFormat[PipegraphInstanceModel] = jsonFormat8(
    PipegraphInstanceModel.apply
  )
  implicit lazy val documentModelFormat: RootJsonFormat[DocumentModel]     = jsonFormat3(DocumentModel.apply)
  implicit lazy val freeCodeModelFormat: RootJsonFormat[FreeCodeModel]     = jsonFormat2(FreeCodeModel.apply)
  implicit lazy val freeCodeFormat: RootJsonFormat[FreeCode]               = jsonFormat1(FreeCode.apply)
  implicit lazy val errorModelFormat: RootJsonFormat[ErrorModel]           = jsonFormat6(ErrorModel.apply)
  implicit lazy val completionModelFormat: RootJsonFormat[CompletionModel] = jsonFormat2(CompletionModel.apply)

  implicit lazy val websocketModelFormat: RootJsonFormat[WebsocketModel] = jsonFormat5(WebsocketModel.apply)

  implicit lazy val jdbcPartitioningInfoFormat: RootJsonFormat[JdbcPartitioningInfo] = jsonFormat3(
    JdbcPartitioningInfo.apply
  )

  implicit lazy val sqlSourceModelFormat: RootJsonFormat[SqlSourceModel] = jsonFormat6(
    SqlSourceModel.apply
  )

  // Editor Format
  implicit lazy val nifiEditorFormat: RootJsonFormat[NifiStatelessInstanceModel] = jsonFormat3(
    NifiStatelessInstanceModel
  )

  implicit lazy val jObjectFormat: RootJsonFormat[JObject] = new RootJsonFormat[JObject] {
    override def write(obj: JObject): JsValue = {
      implicit val format: Formats = DefaultFormats
      org.json4s.jackson.Serialization.write(obj).parseJson
    }

    override def read(json: JsValue): JObject = {
      implicit val format: Formats = DefaultFormats
      org.json4s.jackson.Serialization.read[JObject](json.toString())
    }
  }

  implicit lazy val processGroupResponseFormat: RootJsonFormat[ProcessGroupResponse] = jsonFormat2(ProcessGroupResponse)
  implicit lazy val processGroupModelFormat: RootJsonFormat[ProcessGroupModel]       = jsonFormat3(ProcessGroupModel)

  /*
   Pipegrpaph editor formats
   */
  implicit lazy val pipegraphDTOFormat: RootJsonFormat[PipegraphDTO] = jsonFormat4(PipegraphDTO.apply)
  implicit lazy val structuredStreamingETLDTOFormat: RootJsonFormat[StructuredStreamingETLDTO] = jsonFormat7(
    StructuredStreamingETLDTO.apply
  )

  implicit lazy val errorDTOFormat: RootJsonFormat[ErrorDTO] = jsonFormat1(ErrorDTO.apply)

  // Strategy DTO formats
  implicit lazy val freeCodeDTOFormat: RootJsonFormat[FreeCodeDTO]           = jsonFormat3(FreeCodeDTO)
  implicit lazy val flowNifiDTOFormat: RootJsonFormat[FlowNifiDTO]           = jsonFormat3(FlowNifiDTO)
  implicit lazy val strategyClassDTOFormat: RootJsonFormat[StrategyClassDTO] = jsonFormat2(StrategyClassDTO)

  implicit lazy val strategyDTOFormat: RootJsonFormat[StrategyDTO] = new RootJsonFormat[StrategyDTO] {
    override def read(json: JsValue): StrategyDTO =
      json
        .asJsObject("StrategyDTO should be an JSON Object")
        .getFields("strategyType")
        .headOption match {
        case Some(JsString(StrategyDTO.freecodeType)) => freeCodeDTOFormat.read(json)
        case Some(JsString(StrategyDTO.nifiType))     => flowNifiDTOFormat.read(json)
        case Some(JsString(StrategyDTO.codebaseType)) => strategyClassDTOFormat.read(json)
        case Some(_)                                  => deserializationError(s"$json is not a StrategyDTO subclass")
        case None                                     => deserializationError(s"$json it's missing a strategyType field")
        case _                                        => deserializationError(s"$json It's not a valid StrategyDTO")
      }

    override def write(obj: StrategyDTO): JsValue = obj match {
      case sb: FreeCodeDTO =>
        JsObject(
          freeCodeDTOFormat.write(sb).asJsObject.fields +
            ("outputType" -> JsString(StrategyDTO.freecodeType))
        )
      case sb: FlowNifiDTO =>
        JsObject(
          flowNifiDTOFormat.write(sb).asJsObject.fields +
            ("outputType" -> JsString(StrategyDTO.nifiType))
        )
      case sb: StrategyClassDTO =>
        JsObject(
          strategyClassDTOFormat.write(sb).asJsObject.fields +
            ("outputType" -> JsString(StrategyDTO.codebaseType))
        )
    }
  }

  // IO DTO formats
  implicit lazy val rawModelSetupDTOFormat: RootJsonFormat[RawModelSetupDTO] = jsonFormat7(RawModelSetupDTO)

  implicit lazy val topicModelDTOFormat: RootJsonFormat[TopicModelDTO] = jsonFormat1(TopicModelDTO)
  implicit lazy val indexModelDTOFormat: RootJsonFormat[IndexModelDTO] = jsonFormat1(IndexModelDTO)
  implicit lazy val kvModelDTOFormat: RootJsonFormat[KeyValueModelDTO] = jsonFormat1(KeyValueModelDTO)
  implicit lazy val rawModelDTOFormat: RootJsonFormat[RawModelDTO]     = jsonFormat2(RawModelDTO)

  implicit lazy val datastoreDTOFormat: RootJsonFormat[DatastoreModelDTO] = new RootJsonFormat[DatastoreModelDTO] {
    override def read(json: JsValue): DatastoreModelDTO =
      json
        .asJsObject("Datastore DTO should be an JSON Object")
        .getFields("modelType")
        .headOption match {
        case Some(JsString(DatastoreModelDTO.topicType))    => topicModelDTOFormat.read(json)
        case Some(JsString(DatastoreModelDTO.indexType))    => indexModelDTOFormat.read(json)
        case Some(JsString(DatastoreModelDTO.keyValueType)) => kvModelDTOFormat.read(json)
        case Some(JsString(DatastoreModelDTO.rawDataType))  => rawModelDTOFormat.read(json)
        case Some(_)                                        => deserializationError(s"$json is not a DatastoreDTO subclass")
        case None                                           => deserializationError(s"$json it's missing a modelType field")
        case _                                              => deserializationError(s"$json It's not a valid DatastoreDTO")
      }

    override def write(obj: DatastoreModelDTO): JsValue = obj match {
      case sb: TopicModelDTO =>
        JsObject(
          topicModelDTOFormat.write(sb).asJsObject.fields +
            ("modelType" -> JsString(DatastoreModelDTO.topicType))
        )
      case sb: IndexModelDTO =>
        JsObject(
          indexModelDTOFormat.write(sb).asJsObject.fields +
            ("modelType" -> JsString(DatastoreModelDTO.indexType))
        )
      case sb: KeyValueModelDTO =>
        JsObject(
          kvModelDTOFormat.write(sb).asJsObject.fields +
            ("modelType" -> JsString(DatastoreModelDTO.keyValueType))
        )
      case sb: RawModelDTO =>
        JsObject(
          rawModelDTOFormat.write(sb).asJsObject.fields +
            ("modelType" -> JsString(DatastoreModelDTO.rawDataType))
        )
    }
  }

  implicit lazy val readerModelDTOFormat: RootJsonFormat[ReaderModelDTO] = jsonFormat4(ReaderModelDTO.apply)
  implicit lazy val writerModelDTOFormat: RootJsonFormat[WriterModelDTO] = jsonFormat3(WriterModelDTO.apply)
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
