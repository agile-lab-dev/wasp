package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.KeyValueBL
import it.agilelab.bigdata.wasp.models.KeyValueModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString

class KeyValueBLImp(waspDB: WaspMongoDB) extends KeyValueBL {

  private def factory(t: KeyValueModel) =
    KeyValueModel(t.name, t.tableCatalog, t.dataFrameSchema, t.options, t.useAvroSchemaManager, t.avroSchemas)

  def getByName(name: String): Option[KeyValueModel] = {
    waspDB
      .getDocumentByField[KeyValueModel]("name", new BsonString(name))
      .map(index => {
        factory(index)
      })
  }

  override def persist(rawModel: KeyValueModel): Unit = waspDB.insert[KeyValueModel](rawModel)

  override def upsert(rawModel: KeyValueModel): Unit = waspDB.upsert[KeyValueModel](rawModel)

  override def getAll(): Seq[KeyValueModel] = waspDB.getAll[KeyValueModel]
}
