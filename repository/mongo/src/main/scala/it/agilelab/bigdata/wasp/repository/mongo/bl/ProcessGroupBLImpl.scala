package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.ProcessGroupBL
import it.agilelab.bigdata.wasp.models.ProcessGroupModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.ProcessGroupDBModel
import it.agilelab.bigdata.wasp.repository.core.mappers.ProcessGroupMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.ProcessGroupMapperV1.fromModelToDBModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.BsonString

class ProcessGroupBLImpl(waspDB: WaspMongoDB) extends ProcessGroupBL {

  override def getById(pgId: String): Option[ProcessGroupModel] =
    waspDB
      .getAllDocumentsByField[ProcessGroupDBModel]("name", BsonString(pgId))
      .headOption.map(applyMap)

  override def insert(processGroup: ProcessGroupModel): Unit =
    waspDB.insert[ProcessGroupDBModel](fromModelToDBModel(processGroup))

  override def upsert(processGroup: ProcessGroupModel): Unit =
    waspDB.upsert[ProcessGroupDBModel](fromModelToDBModel(processGroup))
}
