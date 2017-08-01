package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{BSONConversionHelper, MlModelOnlyInfo}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.apache.commons.lang3.SerializationUtils
import play.api.libs.iteratee.Enumerator
import reactivemongo.api.gridfs._
import reactivemongo.bson._
import reactivemongo.api.commands._
import reactivemongo.api.BSONSerializationPack

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

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
  def getById(id: String): Future[Option[MlModelOnlyInfo]]

  /**
   * Find the most recent model with this name and version
    *
    * @param name model name
   * @param version model version
   * @return info of the model
   */
  def getMlModelOnlyInfo(name: String, version: String): Future[Option[MlModelOnlyInfo]]

  /**
   * Find a precise model that is identify by name, version and timestamp
    *
    * @param name
   * @param version
   * @param timestamp
   * @return
   */
  def getMlModelOnlyInfo(name: String, version: String, timestamp: Long): Future[Option[MlModelOnlyInfo]]

  /**
   * Get all model saved
    *
    * @return
   */
  def getAll: Future[List[MlModelOnlyInfo]]

  /**
   * Get an Enumerator with the model already deserialized
   * the mlModelOnlyInfo must have initialized
    *
    * @param mlModelOnlyInfo All the metadata about the model with the modelFileId initialized
   * @return
   */
  def getSerializedTransformer(mlModelOnlyInfo: MlModelOnlyInfo): Future[Option[Enumerator[Any]]]

  /**
   * Persist only the metadata about the model
    *
    * @param mlModelOnlyInfo
   * @return
   */
  def saveMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Future[WriteResult]

  /**
   * Persist the transformer model
    *
    * @param transformerModel
   * @param name
   * @param version
   * @param timestamp
   * @return the id of the model
   */
  def saveTransformer(transformerModel: Serializable, name: String, version: String, timestamp: Long): Future[BSONObjectID]

  /**
   * Delete the metadata and the transformer model
    *
    * @param id The id of the metadata document content
   * @return
   */
  def delete(id: String): Future[Option[WriteResult]]

  /**
   * Delete the metadata and the transformer model in base to name, version, timestamp
    *
    * @param name
   * @param version
   * @param timestamp
   * @return
   */
  def delete(name: String, version: String, timestamp: Long): Future[Option[WriteResult]]

  /**
   * Update only the metadata about the model
    *
    * @param mlModelOnlyInfo
   * @return
   */
  def updateMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Future[WriteResult]

}

/**
 * The metadata e the model are saved in two different area:
 * The metadata area a collection
 * The models are serializable in GridFS
 * The keys to identify one metadata is _id or name, version, timestamp
 * The key to identify one model is _id that match with modelFileId in metadata object
 */
class MlModelBLImp(waspDB: WaspDB) extends MlModelBL with BSONConversionHelper {


  def getMlModelOnlyInfo(name: String, version: String): Future[Option[MlModelOnlyInfo]] = {

    getMlModelOnlyInfo(Map(
      "name" -> new BSONString(name),
      "version" -> new BSONString(version),
      "$orderby" -> BSONDocument(Map(
        "timestamp" -> new BSONLong(-1)
      ))
    ))
  }

  def getMlModelOnlyInfo(name: String, version: String, timestamp: Long): Future[Option[MlModelOnlyInfo]] = {

    getMlModelOnlyInfo(Map(
      "name" -> new BSONString(name),
      "version" -> new BSONString(version),
      "timestamp" -> BSONTimestamp(timestamp)
    ))
  }

  private def getMlModelOnlyInfo(queryParams: Map[String, BSONValue]): Future[Option[MlModelOnlyInfo]] = {
    waspDB.getDocumentByQueryParams[MlModelOnlyInfo](queryParams)
  }

  override def getAll: Future[List[MlModelOnlyInfo]] = {
    waspDB.getAll[MlModelOnlyInfo]
  }
  def getSerializedTransformer(mlModelOnlyInfo: MlModelOnlyInfo): Future[Option[Enumerator[Any]]] = {
    import scala.concurrent.ExecutionContext.Implicits.global
    if (mlModelOnlyInfo.modelFileId.isDefined) {
      val futureGridFsFile = waspDB.getFileByID(mlModelOnlyInfo.modelFileId.get)

      futureGridFsFile.map(fileOpt => {
        fileOpt.map((file: ReadFile[BSONSerializationPack.type, BSONValue]) => {
          waspDB.enumerateFile(file).map(enumerateArrayBytes => {
            SerializationUtils.deserialize[Any](enumerateArrayBytes)
          })
        })
      })
    } else {
      Future.apply(None)
    }
  }
  def saveMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Future[WriteResult] = {
    waspDB.insert(mlModelOnlyInfo)
  }
  def saveTransformer(transformerModel: Serializable, name: String, version: String, timestamp: Long): Future[BSONObjectID] = {
    val serialized = SerializationUtils.serialize(transformerModel)
    val contentType = "application/octet-stream"
    val fileToSave = DefaultFileToSave(s"$name-$version-$timestamp", Some(contentType))
    waspDB.saveFile(serialized, fileToSave).map(fileSaved => fileSaved.id.asInstanceOf[BSONObjectID])
  }

  override def getById(id: String): Future[Option[MlModelOnlyInfo]] = waspDB.getDocumentByID[MlModelOnlyInfo](BSONObjectID(id))

  override def delete(id: String): Future[Option[WriteResult]] = {
    val infoOptFuture = waspDB.getDocumentByID[MlModelOnlyInfo](BSONObjectID(id))
      infoOptFuture.flatMap {
        case Some(info)  =>
          if(info.modelFileId.isDefined) {
              waspDB.deleteById[MlModelOnlyInfo](info._id.get)
              waspDB.deleteFileById(info.modelFileId.get).map(Some(_))
            }
          else{
            waspDB.deleteById[MlModelOnlyInfo](info._id.get).map(Some(_))
          }
        case None => Future(None)
      }
  }

  override def delete(name: String, version: String, timestamp: Long): Future[Option[WriteResult]] = {
    val infoOptFuture = getMlModelOnlyInfo(name, version, timestamp)
    infoOptFuture.flatMap {
      case Some(info)  =>
        if(info.modelFileId.isDefined) {
          waspDB.deleteById[MlModelOnlyInfo](info._id.get)
          waspDB.deleteFileById(info.modelFileId.get).map(Some(_))
        }
        else {
          waspDB.deleteById[MlModelOnlyInfo](info._id.get).map(Some(_))
        }
      case None => Future(None)
    }
  }

  def updateMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo) = {
    waspDB.updateById[MlModelOnlyInfo](mlModelOnlyInfo._id.get, mlModelOnlyInfo)
  }
}