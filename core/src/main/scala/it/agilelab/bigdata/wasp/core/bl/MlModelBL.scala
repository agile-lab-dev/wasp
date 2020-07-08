package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.MlModelOnlyInfo
import org.mongodb.scala.bson.BsonObjectId

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

  def getFileByID(mlModelOnlyInfo: MlModelOnlyInfo): Option[Array[Byte]]


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