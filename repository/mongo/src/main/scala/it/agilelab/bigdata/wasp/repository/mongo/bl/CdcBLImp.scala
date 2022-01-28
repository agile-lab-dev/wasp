package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models.CdcModel
import it.agilelab.bigdata.wasp.repository.core.bl.{CdcBL, RawBL}
import it.agilelab.bigdata.wasp.repository.core.dbModels.{CdcDBModel, CdcDBModelV1}
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString
import it.agilelab.bigdata.wasp.repository.core.mappers.CdcMapperSelector.factory
import it.agilelab.bigdata.wasp.repository.core.mappers.CdcMapperV1.transform

class CdcBLImp(waspDB: WaspMongoDB) extends CdcBL  {


  def getByName(name: String): Option[CdcModel] = {
    waspDB.getDocumentByField[CdcDBModel]("name", new BsonString(name)).map(factory)
  }

  override def getAll(): Seq[CdcModel] =
    waspDB.getAll[CdcDBModel].map(factory)

  override def persist(cdcModel: CdcModel): Unit =
    waspDB.insert[CdcDBModel](transform[CdcDBModelV1](cdcModel))

  override def upsert(cdcModel: CdcModel): Unit =
    waspDB.upsert[CdcDBModel](transform[CdcDBModelV1](cdcModel))
}
