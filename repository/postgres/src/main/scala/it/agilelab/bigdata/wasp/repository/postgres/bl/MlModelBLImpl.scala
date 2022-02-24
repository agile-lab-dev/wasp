package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.MlModelOnlyInfo
import it.agilelab.bigdata.wasp.repository.core.bl.MlModelBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{MlModelOnlyDataTableDefinition, MlModelOnlyInfoTableDefinition, TableDefinition}
import org.bson.types.ObjectId
import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}

import java.sql.ResultSet

case class MlModelBLImpl(waspDB: WaspPostgresDB) extends MlModelBL with PostgresBL{

  override implicit val tableDefinition: TableDefinition[MlModelOnlyInfo, (String,String,Long)] = MlModelOnlyInfoTableDefinition
  val tableData : TableDefinition[(BsonObjectId,String,BsonDocument,Array[Byte]),BsonObjectId] = MlModelOnlyDataTableDefinition



    /**
    * Find the most recent model with this name and version
    *
    * @param name    model name
    * @param version model version
    * @return info of the model
    */
  override def getMlModelOnlyInfo(name: String, version: String): Option[MlModelOnlyInfo] = {
    waspDB.getBy(Array((MlModelOnlyInfoTableDefinition.name,name) ,(MlModelOnlyInfoTableDefinition.version, version)),
      Some(s"${MlModelOnlyInfoTableDefinition.timestamp} DESC"),Some(1))(tableDefinition).headOption
  }

  /**
    * Find a precise model that is identify by name, version and timestamp
    *
    * @param name
    * @param version
    * @param timestamp
    * @return
    */
  override def getMlModelOnlyInfo(name: String, version: String, timestamp: Long): Option[MlModelOnlyInfo] =
    waspDB.getByPrimaryKey(Tuple3(name,version,timestamp))(tableDefinition)


  /**
    * Get all model saved
    *
    * @return
    */
  override def getAll: Seq[MlModelOnlyInfo] = waspDB.getAll()


  override def getFileByID(mlModelOnlyInfo: MlModelOnlyInfo): Option[Array[Byte]] = {
    mlModelOnlyInfo.modelFileId.flatMap{id=>
      waspDB.getByPrimaryKey(id)(tableData).map(_._4)}
  }


  /**
    * Persist only the metadata about the model
    *
    * @param mlModelOnlyInfo
    * @return
    */
  override def saveMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Unit =
    waspDB.insert(mlModelOnlyInfo)(tableDefinition)



  /**
    * Delete the metadata and the transformer model in base to name, version, timestamp
    *
    * @param name
    * @param version
    * @param timestamp
    * @return
    */
  override def delete(name: String, version: String, timestamp: Long): Unit = {
    val infoOptFuture: Option[MlModelOnlyInfo] = getMlModelOnlyInfo(name, version, timestamp)
    infoOptFuture.foreach(info => {
      if (info.modelFileId.isDefined) {
        waspDB.deleteByPrimaryKey(Tuple3(name,version,timestamp))
        waspDB.deleteByPrimaryKey(info.modelFileId.get)(tableData)
      } else {
        waspDB.deleteByPrimaryKey(Tuple3(name,version,timestamp))
      }
    })
  }

  /**
    * Update only the metadata about the model
    *
    * @param mlModelOnlyInfo
    * @return
    */
  override def updateMlModelOnlyInfo(mlModelOnlyInfo: MlModelOnlyInfo): Unit =
    waspDB.updateByPrimaryKey(mlModelOnlyInfo)(tableDefinition)

  override protected def saveFile(file: Array[Byte], fileName: String, metadata: BsonDocument): BsonObjectId = {
   val table = MlModelOnlyDataTableDefinition
    val mapper : ResultSet => BsonObjectId = rs => new BsonObjectId(new ObjectId("%024d".format(rs.getLong(table.id))))
    val obj : (BsonObjectId,String,BsonDocument,Array[Byte])= (null, fileName, metadata, file)
    waspDB.insertReturning(obj,Array(table.id),mapper)(table).head
  }

  override def createTable(): Unit = {
    waspDB.createTable()(tableDefinition)
    waspDB.createTable()(MlModelOnlyDataTableDefinition)
  }
}
