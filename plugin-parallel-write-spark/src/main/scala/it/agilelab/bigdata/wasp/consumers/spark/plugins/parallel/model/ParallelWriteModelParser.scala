package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.models.GenericModel
import spray.json.DefaultJsonProtocol._
import spray.json._

/**
  * Parallel write flavours encoders/decoders for spray
  */
object ParallelWriteModelParser {
  implicit lazy val parallelWriteFormat: RootJsonFormat[ParallelWrite] = jsonFormat1((saveMode: String) => ParallelWrite.apply(saveMode))
  implicit lazy val catalogCoordinatesFormat: RootJsonFormat[CatalogCoordinates] = jsonFormat5(CatalogCoordinates.apply)
  implicit lazy val continuousUpdateFormat: RootJsonFormat[ContinuousUpdate] = jsonFormat2((keys: List[String], orderingExpression: String) => ContinuousUpdate.apply(keys, orderingExpression))

  implicit lazy val writerDetailsFormat: RootJsonFormat[WriterDetails] = new RootJsonFormat[WriterDetails] {
    override def read(json: JsValue): WriterDetails =
      json
        .asJsObject("Type must be a JSON object")
        .getFields("writerType")
        .headOption match {
        case Some(JsString(WriterDetails.parallelWrite)) => parallelWriteFormat.read(json)
        case Some(JsString(WriterDetails.continuousUpdate)) => continuousUpdateFormat.read(json)
        case Some(_) => deserializationError(s"$json is not a WriterDetails subclass")
        case None => deserializationError(s"$json it's missing a writerType field")
        case _ => deserializationError(s"$json It's not a valid WriterDetails")
      }

    override def write(obj: WriterDetails): JsValue = obj match {
      case coldArea: ParallelWrite =>
        JsObject(
          parallelWriteFormat.write(coldArea).asJsObject.fields +
            ("writerType" -> JsString(WriterDetails.parallelWrite))
        )
      case continuousUpdate: ContinuousUpdate =>
        JsObject(
          continuousUpdateFormat.write(continuousUpdate).asJsObject.fields +
            ("writerType" -> JsString(WriterDetails.continuousUpdate))
        )
    }
  }


  implicit lazy val parallelWriteModeFormat: RootJsonFormat[ParallelWriteModel] = new RootJsonFormat[ParallelWriteModel] {
    override def write(obj: ParallelWriteModel): JsValue = {
      JsObject(
        "writerDetails" -> writerDetailsFormat.write(obj.writerDetails),
        "entityDetails" -> obj.entityDetails.toJson
      )
    }

    private def parseEntityDetails(entityDetails: Map[String, String]): CatalogCoordinates = {
      CatalogCoordinates(
        entityDetails.getOrElse("domain", ""),
        entityDetails.getOrElse("name", ""),
        entityDetails.getOrElse("version", "")
      )
    }

    override def read(json: JsValue): ParallelWriteModel = {
        val fields = json.asJsObject("Values must be a JSON Object").fields
        val parallelWriteModel = for {
          writerDetailType <- fields.get("writerDetails")
          entityDetails <- fields.get("entityDetails")
        } yield ParallelWriteModel(writerDetailsFormat.read(writerDetailType), parseEntityDetails(entityDetails.convertTo[Map[String, String]]))
      parallelWriteModel match {
        case Some(parallelWriteModel) => parallelWriteModel
        case None => throw new Exception(s"$json is not a valid ParallelWriteModel" )
      }
    }
  }

  def parseParallelWriteModel(genericModel: GenericModel): ParallelWriteModel = {
    if (genericModel.product.categoryName == "parallelWrite") {
      val json = genericModel.value.toJson.parseJson
        parallelWriteModeFormat.read(json)
    } else throw new IllegalArgumentException(s"""Expected value of GenericModel.kind is "parallelWrite", found ${genericModel.value}""")
  }
}
