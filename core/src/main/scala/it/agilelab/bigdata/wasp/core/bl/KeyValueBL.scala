package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.KeyValueModel
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonString
import org.mongodb.scala.bson.BsonObjectId

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

trait KeyValueBL {

  def getByName(name: String): Option[KeyValueModel]

  def getAll(): Seq[KeyValueModel]

  def persist(rawModel: KeyValueModel): Unit
}

class KeyValueBLImp(waspDB: WaspDB) extends KeyValueBL {

  private def factory(t: KeyValueModel) =
    KeyValueModel(t.name, t.tableCatalog, t.dataFrameSchema, t.options, t.useAvroSchemaManager, t.avroSchemas)

  def getByName(name: String) = {
    waspDB
      .getDocumentByField[KeyValueModel]("name", new BsonString(name))
      .map(index => {
        factory(index)
      })
  }

  override def persist(rawModel: KeyValueModel): Unit = waspDB.insert[KeyValueModel](rawModel)

  override def getAll(): Seq[KeyValueModel] = waspDB.getAll[KeyValueModel]
}
