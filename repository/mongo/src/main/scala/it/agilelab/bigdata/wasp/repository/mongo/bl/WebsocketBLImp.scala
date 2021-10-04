package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.WebsocketBL
import it.agilelab.bigdata.wasp.models.WebsocketModel
import it.agilelab.bigdata.wasp.repository.core.dbModels.WebsocketDBModel
import it.agilelab.bigdata.wasp.repository.core.mappers.WebsocketMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.WebsocketMapperV1.fromModelToDBModel
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.bson.BsonString

class WebsocketBLImp(waspDB: WaspMongoDB) extends WebsocketBL  {

  def getByName(name: String): Option[WebsocketModel] = {
    waspDB.getDocumentByField[WebsocketDBModel]("name", new BsonString(name)).map(applyMap)
  }

  override def persist(wsModel: WebsocketModel): Unit =
    waspDB.insert[WebsocketDBModel](fromModelToDBModel(wsModel))
}
