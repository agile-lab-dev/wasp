package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.{GenericModel}
import it.agilelab.bigdata.wasp.repository.core.bl.{GenericBL}
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString

class GenericBLImp(waspDB: WaspMongoDB) extends GenericBL  {

  private def factory(t: GenericModel) = new GenericModel(t.name, t.value, t.product)

  def getByName(name: String) = {
    waspDB.getDocumentByField[GenericModel]("name", new BsonString(name)).map(factory)
  }

  override def getAll(): Seq[GenericModel] = waspDB.getAll[GenericModel]

  override def persist(genericModel: GenericModel): Unit = waspDB.insert[GenericModel](genericModel)

  override def upsert(genericModel: GenericModel): Unit = waspDB.upsert[GenericModel](genericModel)
}
