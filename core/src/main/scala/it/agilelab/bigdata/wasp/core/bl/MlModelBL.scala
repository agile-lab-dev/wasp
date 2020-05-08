package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{MlModelOnlyInfo, ProducerModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.commons.lang3.SerializationUtils
import org.mongodb.scala.bson.{BsonDocument, BsonInt64, BsonObjectId, BsonString, BsonValue}

/**
  * This class allow to read and persist the machine learning models
  */
trait MlModelBL {

  /**
    * Find the most recent model with this name and version
    *
    * @param name model name
    * @param version model version
    * @return info of the model
    */
  def getMlModelOnlyInfo(name: String, version: String): Option[MlModelOnlyInfo]

  /**
    * Find a precise model that is identify by name, version and timestamp
    *
    * @param name
    * @param version
    * @param timestamp
    * @return
    */
  def getMlModelOnlyInfo(name: String, version: String, timestamp: Long): Option[MlModelOnlyInfo]

  /**
    * Get all model saved
    *
    * @return
    */
  def getAll: Seq[MlModelOnlyInfo]

  /**
    * Get an Enumerator with the model already deserialized
    * the mlModelOnlyInfo must have initialized
    *
    * @param mlModelOnlyInfo All the metadata about the model with the modelFileId initialized
    * @return
    */
  def getSerializedTransformer(mlModelOnlyInfo: MlModelOnlyInfo): Option[Any]

  /**
    * Persist only the metadata about the model
    *
    * @param mlModelOnlyInfo
    * @return
    */
  def saveMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Unit

  /**
    * Persist the transformer model
    *
    * @param transformerModel
    * @param name
    * @param version
    * @param timestamp
    * @return the id of the model
    */
  def saveTransformer(transformerModel: Serializable, name: String, version: String, timestamp: Long): BsonObjectId

  /**
    * Delete the metadata and the transformer model in base to name, version, timestamp
    *
    * @param name
    * @param version
    * @param timestamp
    * @return
    */
  def delete(name: String, version: String, timestamp: Long): Unit

  /**
    * Update only the metadata about the model
    *
    * @param mlModelOnlyInfo
    * @return
    */
  def updateMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Unit

}

/**
  * The metadata e the model are saved in two different area:
  * The metadata area a collection
  * The models are serializable in GridFS
  * The keys to identify one metadata is _id or name, version, timestamp
  * The key to identify one model is _id that match with modelFileId in metadata object
  */
class MlModelBLImp(waspDB: WaspDB) extends MlModelBL {

  def getMlModelOnlyInfo(name: String, version: String): Option[MlModelOnlyInfo] = {

    getMlModelOnlyInfo(
      Map(
        "name"    -> new BsonString(name),
        "version" -> new BsonString(version)
      ),
      Some(
        BsonDocument(
          Map(
            "timestamp" -> new BsonInt64(-1)
          )
        )
      )
    )
  }

  def getMlModelOnlyInfo(name: String, version: String, timestamp: Long): Option[MlModelOnlyInfo] = {

    getMlModelOnlyInfo(
      Map(
        "name"      -> BsonString(name),
        "version"   -> BsonString(version),
        "timestamp" -> BsonInt64(timestamp)
      ),
      None
    )
  }

  private def getMlModelOnlyInfo(
      queryParams: Map[String, BsonValue],
      sort: Option[BsonDocument]
  ): Option[MlModelOnlyInfo] = {
    waspDB.getDocumentByQueryParams[MlModelOnlyInfo](queryParams, sort)
  }

  override def getAll: Seq[MlModelOnlyInfo] = {
    waspDB.getAll[MlModelOnlyInfo]()
  }

  /**
    * Pay attention this method could not working
    * @param mlModelOnlyInfo All the metadata about the model with the modelFileId initialized
    * @return
    */
  def getSerializedTransformer(mlModelOnlyInfo: MlModelOnlyInfo): Option[Any] = {
    if (mlModelOnlyInfo.modelFileId.isDefined) {
      val futureGridFsFile = waspDB.getFileByID(mlModelOnlyInfo.modelFileId.get)
      Some(SerializationUtils.deserialize[Any](futureGridFsFile))
    } else {
      None
    }
  }
  def saveMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Unit = {
    waspDB.insert(mlModelOnlyInfo)
  }
  def saveTransformer(transformerModel: Serializable, name: String, version: String, timestamp: Long): BsonObjectId = {
    val serialized  = SerializationUtils.serialize(transformerModel)
    val contentType = "application/octet-stream"
    val metadata    = BsonDocument(Map("contentType" -> BsonString(contentType)))
    waspDB.saveFile(serialized, s"$name-$version-$timestamp", metadata)
  }

  override def delete(name: String, version: String, timestamp: Long): Unit = {
    val infoOptFuture: Option[MlModelOnlyInfo] = getMlModelOnlyInfo(name, version, timestamp)
    infoOptFuture.foreach(info => {
      if (info.modelFileId.isDefined) {
        waspDB.deleteByName[MlModelOnlyInfo](info.name)
        waspDB.deleteFileById(info.modelFileId.get)
      } else {
        waspDB.deleteByName[MlModelOnlyInfo](info.name)
      }
    })
  }

  def updateMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo) = {
    if (waspDB.updateByName[MlModelOnlyInfo](mlModelOnlyInfo.name, mlModelOnlyInfo).getMatchedCount != 1) {
      throw new RuntimeException(s"Model with name ${mlModelOnlyInfo.name} to update not found")
    }
  }
}
