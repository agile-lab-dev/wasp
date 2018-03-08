package it.agilelab.bigdata.wasp.master.web.utils

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration._
import it.agilelab.bigdata.wasp.core.utils.{ConnectionConfig, KryoSerializerConfig, SparkDriverConfig, ZookeeperConnectionsConfig}
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
      case _ => deserializationError("Color expected")
    }
  }
}


/**
  * Based on the code found: https://groups.google.com/forum/#!topic/spray-user/RkIwRIXzDDc
  */
class EnumJsonConverter[T <: scala.Enumeration](enu: T) extends RootJsonFormat[T#Value] {

  import spray.json.{DeserializationException, JsString, JsValue}

  override def write(obj: T#Value): JsValue = JsString(obj.toString)

  override def read(json: JsValue): T#Value = {
    json match {
      case JsString(txt) => enu.withName(txt)
      case somethingElse => throw DeserializationException(s"Expected a value from enum $enu instead of $somethingElse")
    }
  }
}

// collect your json format instances into a support trait:
trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  import it.agilelab.bigdata.wasp.master.web.utils.BsonConvertToSprayJson._
  implicit val topicModelFormat: RootJsonFormat[TopicModel] = jsonFormat7(TopicModel.apply)
  implicit val indexModelFormat: RootJsonFormat[IndexModel] = jsonFormat8(IndexModel.apply)
  implicit val readerTypeFormat: RootJsonFormat[ReaderType] = jsonFormat2(ReaderType.apply)
  implicit val readerModelFormat: RootJsonFormat[ReaderModel] = jsonFormat3((name: String, endpointName: String, readerType: ReaderType) => ReaderModel.apply(name, endpointName, readerType))
  implicit val writerTypeFormat: RootJsonFormat[WriterType] = jsonFormat2(WriterType.apply)
  implicit val writerModelFormat: RootJsonFormat[WriterModel] = jsonFormat3((name: String, endpointName: Option[String], writerType: WriterType) => WriterModel.apply(name, endpointName, writerType))
  implicit val mlModelOnlyInfoFormat: RootJsonFormat[MlModelOnlyInfo] = jsonFormat8(MlModelOnlyInfo.apply)
  implicit val strategyModelFormat: RootJsonFormat[StrategyModel] = jsonFormat2(StrategyModel.apply)
  implicit val dashboardModelFormat: RootJsonFormat[DashboardModel] = jsonFormat2(DashboardModel.apply)
  implicit val etlModelFormat: RootJsonFormat[LegacyStreamingETLModel] = jsonFormat8(LegacyStreamingETLModel.apply)
  implicit val etlStructuredModelFormat: RootJsonFormat[StructuredStreamingETLModel] = jsonFormat9(StructuredStreamingETLModel.apply)
  implicit val rTModelFormat: RootJsonFormat[RTModel] = jsonFormat5(RTModel.apply)
  implicit val pipegraphModelFormat: RootJsonFormat[PipegraphModel] = jsonFormat10(PipegraphModel.apply)
  implicit val connectionConfigFormat: RootJsonFormat[ConnectionConfig] = jsonFormat5(ConnectionConfig.apply)
  implicit val zookeeperConnectionFormat: RootJsonFormat[ZookeeperConnectionsConfig] = jsonFormat2(ZookeeperConnectionsConfig.apply)
  implicit val kafkaConfigModelFormat: RootJsonFormat[KafkaConfigModel] = jsonFormat10(KafkaConfigModel.apply)
  implicit val sparkDriverConfigFormat: RootJsonFormat[SparkDriverConfig] = jsonFormat6(SparkDriverConfig.apply)
  implicit val kryoSerializerConfigFormat: RootJsonFormat[KryoSerializerConfig] = jsonFormat3(KryoSerializerConfig.apply)
  implicit val sparkStreamingConfigModelFormat: RootJsonFormat[SparkStreamingConfigModel] = jsonFormat20(SparkStreamingConfigModel.apply)
  implicit val sparkBatchConfigModelFormat: RootJsonFormat[SparkBatchConfigModel] = jsonFormat18(SparkBatchConfigModel.apply)
  implicit val elasticConfigModelFormat: RootJsonFormat[ElasticConfigModel] = jsonFormat2(ElasticConfigModel.apply)
  implicit val solrConfigModelFormat: RootJsonFormat[SolrConfigModel] = jsonFormat2(SolrConfigModel.apply)
  implicit val batchETLModelFormat: RootJsonFormat[BatchETLModel] = jsonFormat8(BatchETLModel.apply)
  implicit val batchJobModelFormat: RootJsonFormat[BatchJobModel] = jsonFormat6(BatchJobModel.apply)
  implicit val producerModelFormat: RootJsonFormat[ProducerModel] = jsonFormat7(ProducerModel.apply)
  implicit val jobStatusFormat: RootJsonFormat[JobStatus.JobStatus] = new EnumJsonConverter(JobStatus)
  implicit val batchJobInstanceModelFormat: RootJsonFormat[BatchJobInstanceModel] = jsonFormat6(BatchJobInstanceModel.apply)
}

