package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.RawBL
import it.agilelab.bigdata.wasp.models.RawModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString

class RawBLImp(waspDB: WaspMongoDB) extends RawBL  {

  private def factory(t: RawModel) = new RawModel(t.name, t.uri, t.timed, t.schema, t.options)

  def getByName(name: String) = {
    waspDB.getDocumentByField[RawModel]("name", new BsonString(name)).map(factory)
  }

  override def getAll(): Seq[RawModel] = waspDB.getAll[RawModel]

  override def persist(rawModel: RawModel): Unit = waspDB.insert[RawModel](rawModel)

  override def upsert(rawModel: RawModel): Unit = waspDB.upsert[RawModel](rawModel)
}
