package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.CdcModel
import it.agilelab.bigdata.wasp.repository.core.bl.{CdcBL, RawBL}
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString

class CdcBLImp(waspDB: WaspMongoDB) extends CdcBL  {

  private def factory(t: CdcModel) = CdcModel(t.name, t.uri, t.schema, t.options)

  def getByName(name: String) = {
    waspDB.getDocumentByField[CdcModel]("name", new BsonString(name)).map(factory)
  }

  override def getAll(): Seq[CdcModel] = waspDB.getAll[CdcModel]

  override def persist(cdcModel: CdcModel): Unit = waspDB.insert[CdcModel](cdcModel)

  override def upsert(cdcModel: CdcModel): Unit = waspDB.upsert[CdcModel](cdcModel)
}
