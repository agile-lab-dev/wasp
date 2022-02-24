package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.IndexModel
import it.agilelab.bigdata.wasp.repository.core.bl.IndexBL
import it.agilelab.bigdata.wasp.repository.core.dbModels.{IndexDBModel, IndexDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.IndexDBModelMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.core.mappers.IndexMapperV1.transform
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.BsonString

class IndexBLImp(waspDB: WaspMongoDB) extends IndexBL {

  def getByName(name: String): Option[IndexModel] = {
    waspDB
      .getDocumentByField[IndexDBModel]("name", new BsonString(name))
      .map(factory)
  }

  override def persist(indexModel: IndexModel): Unit =
    waspDB.insert[IndexDBModel](transform[IndexDBModelV1](indexModel))

  override def insertIfNotExists(indexModel: IndexModel): Unit =
    waspDB.insertIfNotExists[IndexDBModel](transform[IndexDBModelV1](indexModel))

  override def upsert(indexModel: IndexModel): Unit =
    waspDB.upsert[IndexDBModel](transform[IndexDBModelV1](indexModel)) // changed bug?

  override def getAll(): Seq[IndexModel] = waspDB.getAll[IndexDBModel]().map(factory)
}
