package it.agilelab.bigdata.wasp.web.utils

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
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
