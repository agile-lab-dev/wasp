package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{BSONConversionHelper, IndexModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import reactivemongo.bson.{BSONObjectID, BSONString}
import reactivemongo.api.commands.WriteResult

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait IndexBL {
  def getByName(name: String): Future[Option[IndexModel]]

  def getById(id: String): Future[Option[IndexModel]]

  def persist(indexModel: IndexModel): Future[WriteResult]

}

class IndexBLImp(waspDB: WaspDB) extends IndexBL with BSONConversionHelper {

  private def factory(t: IndexModel) =
    new IndexModel(t.name, t.creationTime, t.schema, t._id, t.query, t.numShards, t.replicationFactor)

  def getByName(name: String) = {
    waspDB
      .getDocumentByField[IndexModel]("name", new BSONString(name))
      .map(index => {
        index.map(p => factory(p))
      })
  }

  def getById(id: String) = {
    waspDB
      .getDocumentByID[IndexModel](BSONObjectID(id))
      .map(Index => {
        Index.map(p => factory(p))
      })
  }

  override def persist(indexModel: IndexModel): Future[WriteResult] =
    waspDB.insert[IndexModel](indexModel)
}
