package it.agilelab.bigdata.wasp.consumers.spark.MlModels

import it.agilelab.bigdata.wasp.core.models.MlModelOnlyInfo
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.Params
import org.joda.time.DateTime
import org.mongodb.scala.bson.BsonObjectId

/**
 * Created by Mattia Bertorello on 28/09/15.
 */


case class TransformerWithInfo(name: String, version: String,
                               transformer: Transformer with Params,
                               timestamp: Long = DateTime.now().getMillis,
                               favorite: Boolean = false, description: String = "",
                               _id: Option[BsonObjectId] = None,
                               modelFileId: Option[BsonObjectId] = None) {
  val className: String = transformer.getClass.getName
  def toOnlyInfo(modelFileId: BsonObjectId) = {
    MlModelOnlyInfo(_id = _id, name = name, version = version, className = Some(className),
      timestamp = Some(timestamp), favorite = favorite, description = description,
      modelFileId = Some(modelFileId)
    )
  }
  def toOnlyInfo = {
    MlModelOnlyInfo(_id = _id, name = name, version = version, className = Some(className),
      timestamp = Some(timestamp), favorite = favorite, description = description,
      modelFileId = modelFileId)
  }
}

object TransformerWithInfo {
  def create(mlModelOnlyInfo: MlModelOnlyInfo, transformer: Transformer with Params): TransformerWithInfo = {

    TransformerWithInfo(
      _id = mlModelOnlyInfo._id,
      name = mlModelOnlyInfo.name,
      version = mlModelOnlyInfo.version,
      transformer = transformer,
      timestamp = mlModelOnlyInfo.timestamp.getOrElse(DateTime.now().getMillis),
      favorite = mlModelOnlyInfo.favorite,
      description = mlModelOnlyInfo.description,
      modelFileId = mlModelOnlyInfo.modelFileId
    )
  }
}
