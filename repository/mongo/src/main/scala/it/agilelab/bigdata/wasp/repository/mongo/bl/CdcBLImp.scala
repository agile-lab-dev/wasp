package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.CdcModel
import it.agilelab.bigdata.wasp.repository.core.bl.CdcBL
import it.agilelab.bigdata.wasp.repository.core.dbModels.{CdcDBModel, CdcDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.CdcMapperSelector
import it.agilelab.bigdata.wasp.repository.core.mappers.CdcMapperV1.transform
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString

class CdcBLImp(waspDB: WaspMongoDB) extends CdcBL {

  def getByName(name: String): Option[CdcModel] = {
    waspDB.getDocumentByField[CdcDBModel]("name", new BsonString(name)).map(CdcMapperSelector.factory)
  }

  override def getAll(): Seq[CdcModel] =
    waspDB.getAll[CdcDBModel].map(CdcMapperSelector.factory)

  override def persist(cdcModel: CdcModel): Unit =
    waspDB.insert[CdcDBModel](transform[CdcDBModelV1](cdcModel))

  override def upsert(cdcModel: CdcModel): Unit =
    waspDB.upsert[CdcDBModel](transform[CdcDBModelV1](cdcModel))
}
