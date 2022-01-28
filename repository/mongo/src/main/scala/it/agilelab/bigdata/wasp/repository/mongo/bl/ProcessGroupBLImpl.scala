package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.ProcessGroupBL
import it.agilelab.bigdata.wasp.models.ProcessGroupModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{ProcessGroupDBModel, ProcessGroupDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.ProcessGroupMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.core.mappers.ProcessGroupMapperV1.transform
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.BsonString

class ProcessGroupBLImpl(waspDB: WaspMongoDB) extends ProcessGroupBL {

  override def getById(pgId: String): Option[ProcessGroupModel] =
    waspDB
      .getAllDocumentsByField[ProcessGroupDBModel]("name", BsonString(pgId))
      .headOption.map(factory)

  override def insert(processGroup: ProcessGroupModel): Unit =
    waspDB.insert[ProcessGroupDBModel](transform[ProcessGroupDBModelV1](processGroup))

  override def upsert(processGroup: ProcessGroupModel): Unit =
    waspDB.upsert[ProcessGroupDBModel](transform[ProcessGroupDBModelV1](processGroup))
}
