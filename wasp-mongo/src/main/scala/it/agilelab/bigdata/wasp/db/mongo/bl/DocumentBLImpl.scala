package it.agilelab.bigdata.wasp.db.mongo.bl

import it.agilelab.bigdata.wasp.core.bl.DocumentBL
import it.agilelab.bigdata.wasp.core.models.DocumentModel
import it.agilelab.bigdata.wasp.db.mongo.WaspMongoDB
import org.bson.BsonString

class DocumentBLImpl(waspDB: WaspMongoDB) extends DocumentBL  {



  override def getByName(name: String): Option[DocumentModel] = {
    waspDB.getDocumentByField[DocumentModel]("name", new BsonString(name))
  }

  override def getAll(): Seq[DocumentModel] = {
    waspDB.getAll[DocumentModel]()
  }


  override def persist(rawModel: DocumentModel): Unit = waspDB.insert[DocumentModel](rawModel)

  override def upsert(rawModel: DocumentModel): Unit = waspDB.upsert[DocumentModel](rawModel)
}
