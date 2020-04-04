package it.agilelab.bigdata.wasp.master.web.utils

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import com.typesafe.config._
import it.agilelab.bigdata.wasp.core.datastores.{DatastoreProduct, TopicCategory}
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration._
import it.agilelab.bigdata.wasp.core.utils.{ConnectionConfig, DatastoreProductJsonFormat, ZookeeperConnectionsConfig}
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}
import spray.json.{JsValue, RootJsonFormat, _}

/**
  * Created by Agile Lab s.r.l. on 04/08/2017.
  */
object BsonConvertToSprayJson extends SprayJsonSupport with DefaultJsonProtocol {

  implicit object JsonFormatDocument extends RootJsonFormat[BsonDocument] {
    def write(c: BsonDocument): JsValue =  c.toJson.parseJson

    def read(value: JsValue): BsonDocument = BsonDocument(value.toString())
  }

  implicit object JsonFormatObjectId extends RootJsonFormat[BsonObjectId] {
    def write(c: BsonObjectId): JsValue =  c.getValue.toHexString.toJson

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
trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  import it.agilelab.bigdata.wasp.master.web.utils.BsonConvertToSprayJson._

  implicit lazy val telemetryTopicConfigModel : RootJsonFormat[TelemetryTopicConfigModel] = jsonFormat4(TelemetryTopicConfigModel.apply)
  implicit lazy val telemetryConfigModel : RootJsonFormat[TelemetryConfigModel] = jsonFormat4(TelemetryConfigModel.apply)

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



  implicit lazy val mlModelOnlyInfoFormat: RootJsonFormat[MlModelOnlyInfo] = jsonFormat8(MlModelOnlyInfo.apply)
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
  implicit lazy val batchETLModelFormat: RootJsonFormat[BatchETLModel] = jsonFormat8(BatchETLModel.apply)
  implicit lazy val batchJobExclusionConfig: RootJsonFormat[BatchJobExclusionConfig] = jsonFormat2(BatchJobExclusionConfig.apply)
  implicit lazy val batchJobModelFormat: RootJsonFormat[BatchJobModel] = jsonFormat7(BatchJobModel.apply)
  implicit lazy val producerModelFormat: RootJsonFormat[ProducerModel] = jsonFormat7(ProducerModel.apply)
  implicit lazy val jobStatusFormat: RootJsonFormat[JobStatus.JobStatus] = new EnumJsonConverter(JobStatus)
  implicit lazy val typesafeConfigFormat: RootJsonFormat[Config] = new TypesafeConfigJsonConverter
  implicit lazy val batchJobInstanceModelFormat: RootJsonFormat[BatchJobInstanceModel] = jsonFormat7(BatchJobInstanceModel.apply)
  implicit lazy val pipegraphStatusFormat: RootJsonFormat[PipegraphStatus.PipegraphStatus] = new EnumJsonConverter(PipegraphStatus)
  implicit lazy val pipegraphInstanceModelFormat: RootJsonFormat[PipegraphInstanceModel] = jsonFormat6(PipegraphInstanceModel.apply)
}

