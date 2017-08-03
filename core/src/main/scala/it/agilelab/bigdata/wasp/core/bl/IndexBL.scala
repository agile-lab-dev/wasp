package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.mongodb.scala.bson.{BsonObjectId, BsonString}

import scala.concurrent.Future

trait IndexBL {
  def getByName(name: String): Option[IndexModel]

  def getById(id: String): Option[IndexModel]

  def persist(indexModel: IndexModel): Unit

}

class IndexBLImp(waspDB: WaspDB) extends IndexBL {

  private def factory(t: IndexModel) =
    new IndexModel(t.name, t.creationTime, t.schema, t._id, t.query, t.numShards, t.replicationFactor)

  def getByName(name: String) = {
    waspDB
      .getDocumentByField[IndexModel]("name", new BsonString(name))
      .map(factory)
  }

  def getById(id: String) = {
    waspDB
      .getDocumentByID[IndexModel](BsonObjectId(id))
      .map(factory)
  }

  override def persist(indexModel: IndexModel): Unit =
    waspDB.insert[IndexModel](indexModel)
}
