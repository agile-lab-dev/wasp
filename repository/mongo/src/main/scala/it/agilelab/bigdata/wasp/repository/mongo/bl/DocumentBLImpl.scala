package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.DocumentBL
import it.agilelab.bigdata.wasp.models.DocumentModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.DocumentDBModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString
import it.agilelab.bigdata.wasp.repository.core.mappers.DocumentDBModelMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.DocumentMapperV1.fromModelToDBModel

class DocumentBLImpl(waspDB: WaspMongoDB) extends DocumentBL  {



  override def getByName(name: String): Option[DocumentModel] = {
    waspDB.getDocumentByField[DocumentDBModel]("name", new BsonString(name))
      .map(applyMap)
  }

  override def getAll(): Seq[DocumentModel] = {
    waspDB.getAll[DocumentDBModel]().map(applyMap)
  }


  override def persist(model: DocumentModel): Unit =
    waspDB.insert[DocumentDBModel](fromModelToDBModel(model))

  override def upsert(model: DocumentModel): Unit =
    waspDB.upsert[DocumentDBModel](fromModelToDBModel(model))
}
