package it.agilelab.bigdata.wasp.master.web.utils

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import it.agilelab.bigdata.wasp.core.models._
import it.agilelab.bigdata.wasp.core.models.configuration._
import it.agilelab.bigdata.wasp.core.utils.ConnectionConfig
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}
import spray.json.{JsValue, RootJsonFormat}
import spray.json._

/**
  * Created by Agile Lab s.r.l. on 04/08/2017.
  */
object BsonConvertToSprayJson extends SprayJsonSupport with DefaultJsonProtocol{
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
// collect your json format instances into a support trait:
trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  import it.agilelab.bigdata.wasp.master.web.utils.BsonConvertToSprayJson._
  implicit val topicModelFormat: RootJsonFormat[TopicModel] = jsonFormat7(TopicModel.apply)
  implicit val indexModelFormat: RootJsonFormat[IndexModel] = jsonFormat7(IndexModel.apply)
  implicit val readerModelFormat: RootJsonFormat[ReaderModel] = jsonFormat3(ReaderModel.apply)
  implicit val writeTypeFormat: RootJsonFormat[WriteType] = jsonFormat2(WriteType.apply)
  implicit val writerModelFormat: RootJsonFormat[WriterModel] = jsonFormat3(WriterModel.apply)
  implicit val mlModelOnlyInfoFormat: RootJsonFormat[MlModelOnlyInfo] = jsonFormat8(MlModelOnlyInfo.apply)
  implicit val strategyModelFormat: RootJsonFormat[StrategyModel] = jsonFormat2(StrategyModel.apply)
  implicit val dashboardModelFormat: RootJsonFormat[DashboardModel] = jsonFormat2(DashboardModel.apply)
  implicit val etlModelFormat: RootJsonFormat[ETLModel] = jsonFormat8(ETLModel.apply)
  implicit val rTModelFormat: RootJsonFormat[RTModel] = jsonFormat5(RTModel.apply)
  implicit val pipegraphModelFormat: RootJsonFormat[PipegraphModel] = jsonFormat10(PipegraphModel.apply)
  implicit val connectionConfigFormat: RootJsonFormat[ConnectionConfig] = jsonFormat5(ConnectionConfig.apply)
  implicit val kafkaConfigModelFormat: RootJsonFormat[KafkaConfigModel] = jsonFormat10(KafkaConfigModel.apply)
  implicit val sparkBatchConfigModelFormat: RootJsonFormat[SparkBatchConfigModel] = jsonFormat15(SparkBatchConfigModel.apply)
  implicit val sparkStreamingConfigModelFormat: RootJsonFormat[SparkStreamingConfigModel] = jsonFormat17(SparkStreamingConfigModel.apply)
  implicit val elasticConfigModelFormat: RootJsonFormat[ElasticConfigModel] = jsonFormat3(ElasticConfigModel.apply)
  implicit val solrConfigModelFormat: RootJsonFormat[SolrConfigModel] = jsonFormat5(SolrConfigModel.apply)
  implicit val batchJobModelFormat: RootJsonFormat[BatchJobModel] = jsonFormat8(BatchJobModel.apply)
  implicit val producerModelFormat: RootJsonFormat[ProducerModel] = jsonFormat7(ProducerModel.apply)

}
