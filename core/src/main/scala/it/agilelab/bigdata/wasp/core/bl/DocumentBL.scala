package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{DocumentModel, RawModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonString
import org.mongodb.scala.bson.BsonDocument

trait DocumentBL {

  def getByName(name: String): Option[DocumentModel]
  def getAll(): Seq[DocumentModel]
  def persist(rawModel: DocumentModel): Unit
}

class DocumentBLImpl(waspDB: WaspDB) extends DocumentBL  {



  override def getByName(name: String): Option[DocumentModel] = {
    waspDB.getDocumentByField[DocumentModel]("name", new BsonString(name))
  }

  override def getAll(): Seq[DocumentModel] = {
    waspDB.getAll[DocumentModel]()
  }


  override def persist(rawModel: DocumentModel): Unit = waspDB.insert[DocumentModel](rawModel)
}