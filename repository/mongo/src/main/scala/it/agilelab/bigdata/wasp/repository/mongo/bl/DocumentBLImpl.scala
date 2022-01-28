package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.DocumentBL
import it.agilelab.bigdata.wasp.models.DocumentModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.{DocumentDBModel, DocumentDBModelV1}
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString
import it.agilelab.bigdata.wasp.repository.core.mappers.DocumentDBModelMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.core.mappers.DocumentMapperV1.transform

class DocumentBLImpl(waspDB: WaspMongoDB) extends DocumentBL  {



  override def getByName(name: String): Option[DocumentModel] = {
    waspDB.getDocumentByField[DocumentDBModel]("name", new BsonString(name))
      .map(factory)
  }

  override def getAll(): Seq[DocumentModel] = {
    waspDB.getAll[DocumentDBModel]().map(factory)
  }


  override def persist(model: DocumentModel): Unit =
    waspDB.insert[DocumentDBModel](transform[DocumentDBModelV1](model))

  override def upsert(model: DocumentModel): Unit =
    waspDB.upsert[DocumentDBModel](transform[DocumentDBModelV1](model))
}
