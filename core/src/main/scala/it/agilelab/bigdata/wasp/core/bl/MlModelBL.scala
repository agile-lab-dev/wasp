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
   * Get a model by Id
    *
    * @param id BSON Object Id of the model
   * @return info of the model
   */
  def getById(id: String): Option[MlModelOnlyInfo]

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
   * Delete the metadata and the transformer model
    *
    * @param id The id of the metadata document content
   * @return
   */
  def delete(id: String): Unit

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

    getMlModelOnlyInfo(Map(
      "name" -> new BsonString(name),
      "version" -> new BsonString(version),
      "$orderby" -> BsonDocument(Map(
        "timestamp" -> new BsonInt64(-1)
      ))
    ))
  }

  def getMlModelOnlyInfo(name: String, version: String, timestamp: Long): Option[MlModelOnlyInfo] = {

    getMlModelOnlyInfo(Map(
      "name" -> BsonString(name),
      "version" -> BsonString(version),
      "timestamp" -> BsonInt64(timestamp)
    ))
  }

  private def getMlModelOnlyInfo(queryParams: Map[String, BsonValue]): Option[MlModelOnlyInfo] = {
    waspDB.getDocumentByQueryParams[MlModelOnlyInfo](queryParams)
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
    val serialized = SerializationUtils.serialize(transformerModel)
    val contentType = "application/octet-stream"
    val metadata = BsonDocument(Map("contentType" -> BsonString(contentType)))
    waspDB.saveFile(serialized, s"$name-$version-$timestamp", metadata)
  }

  private def factory(p: MlModelOnlyInfo) = MlModelOnlyInfo(p.name, p.version, p.className,
    p.timestamp, p.modelFileId, p.favorite, p.description, p._id)

  override def getById(id: String): Option[MlModelOnlyInfo] =
    waspDB.getDocumentByID[MlModelOnlyInfo](BsonObjectId(id)).map(factory)


  override def delete(id: String): Unit = {
    //TODO Enforce the not deleted model logging
    val infoOptFuture: Option[MlModelOnlyInfo] = waspDB.getDocumentByID[MlModelOnlyInfo](BsonObjectId(id))
      infoOptFuture.foreach(info => {
        if (info.modelFileId.isDefined) {
          waspDB.deleteById[MlModelOnlyInfo](info._id.get)
          waspDB.deleteFileById(info.modelFileId.get)
        }
        else{
          waspDB.deleteById[MlModelOnlyInfo](info._id.get)
        }
      })
  }

  override def delete(name: String, version: String, timestamp: Long): Unit = {
    val infoOptFuture: Option[MlModelOnlyInfo] = getMlModelOnlyInfo(name, version, timestamp)
    infoOptFuture.foreach(info => {
      if (info.modelFileId.isDefined) {
        waspDB.deleteById[MlModelOnlyInfo](info._id.get)
        waspDB.deleteFileById(info.modelFileId.get)
      }
      else{
        waspDB.deleteById[MlModelOnlyInfo](info._id.get)
      }
    })
  }

  def updateMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo) = {
    waspDB.updateById[MlModelOnlyInfo](mlModelOnlyInfo._id.get, mlModelOnlyInfo)
  }
}