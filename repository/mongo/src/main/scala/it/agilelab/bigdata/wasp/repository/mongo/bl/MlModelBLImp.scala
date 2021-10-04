package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.MlModelBL
import it.agilelab.bigdata.wasp.models.MlModelOnlyInfo
import it.agilelab.bigdata.wasp.repository.core.dbModels.MlDBModelOnlyInfo
import it.agilelab.bigdata.wasp.repository.core.mappers.MlDBModelMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.MlDBModelMapperV1.fromModelToDBModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.apache.commons.lang3.SerializationUtils
import org.mongodb.scala.bson.{BsonDocument, BsonInt64, BsonObjectId, BsonString, BsonValue}

/**
  * The metadata e the model are saved in two different area:
  * The metadata area a collection
  * The models are serializable in GridFS
  * The keys to identify one metadata is _id or name, version, timestamp
  * The key to identify one model is _id that match with modelFileId in metadata object
  */
class MlModelBLImp(waspDB: WaspMongoDB) extends MlModelBL {

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
    waspDB.getDocumentByQueryParams[MlDBModelOnlyInfo](queryParams, sort).map(applyMap)
  }

  override def getAll: Seq[MlModelOnlyInfo] = {
    waspDB.getAll[MlDBModelOnlyInfo]().map(applyMap)
  }


  def getFileByID(mlModelOnlyInfo: MlModelOnlyInfo): Option[Array[Byte]] = {
    if (mlModelOnlyInfo.modelFileId.isDefined) {
      Some(waspDB.getFileByID(mlModelOnlyInfo.modelFileId.get))
    } else {
      None
    }
  }
  def saveMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Unit = {
    waspDB.insert[MlDBModelOnlyInfo](fromModelToDBModel(mlModelOnlyInfo))
  }


  protected def saveFile(file : Array[Byte],fileName : String, metadata : BsonDocument): BsonObjectId = {
    waspDB.saveFile(file, fileName, metadata)
  }

  override def delete(name: String, version: String, timestamp: Long): Unit = {
    val infoOptFuture: Option[MlModelOnlyInfo] = getMlModelOnlyInfo(name, version, timestamp)
    infoOptFuture.foreach(info => {
      if (info.modelFileId.isDefined) {
        waspDB.deleteByQuery[MlDBModelOnlyInfo](Map("name" -> BsonString(info.name),
                                                    "version" -> BsonString(info.version),
                                                    "timestamp" -> BsonInt64(info.timestamp.get)))
        waspDB.deleteFileById(info.modelFileId.get)
      }
    })
  }



  def updateMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Unit = {
    if (waspDB.updateByName[MlDBModelOnlyInfo](mlModelOnlyInfo.name, fromModelToDBModel(mlModelOnlyInfo)).getMatchedCount != 1) {
      throw new RuntimeException(s"Model with name ${mlModelOnlyInfo.name} to update not found")
    }
  }
}
