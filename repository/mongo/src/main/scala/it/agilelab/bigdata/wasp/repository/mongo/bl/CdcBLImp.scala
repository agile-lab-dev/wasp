package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.CdcModel
import it.agilelab.bigdata.wasp.repository.core.bl.{CdcBL, RawBL}
import it.agilelab.bigdata.wasp.repository.core.dbModels.CdcDBModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString
import it.agilelab.bigdata.wasp.repository.core.mappers.CdcMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.CdcMapperV1.fromModelToDBModel

class CdcBLImp(waspDB: WaspMongoDB) extends CdcBL  {

  private def factory(t: CdcModel) = CdcModel(t.name, t.uri, t.schema, t.options)

  def getByName(name: String): Option[CdcModel] = {
    waspDB.getDocumentByField[CdcDBModel]("name", new BsonString(name)).map(applyMap)
  }

  override def getAll(): Seq[CdcModel] =
    waspDB.getAll[CdcDBModel].map(applyMap)

  override def persist(cdcModel: CdcModel): Unit =
    waspDB.insert[CdcDBModel](fromModelToDBModel(cdcModel))

  override def upsert(cdcModel: CdcModel): Unit =
    waspDB.upsert[CdcDBModel](fromModelToDBModel(cdcModel))
}
