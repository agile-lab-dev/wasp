package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{WebsocketModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.BsonString
import org.mongodb.scala.bson.BsonObjectId

trait WebsocketBL {
  def getByName(name: String): Option[WebsocketModel]

  def getById(id: String): Option[WebsocketModel]

  def persist(topicModel: WebsocketModel): Unit
}


class WebsocketBLImp(waspDB: WaspDB) extends WebsocketBL  {


  private def factory(ws: WebsocketModel) = new WebsocketModel(ws.name, ws.host, ws.port, ws.resourceName, ws.options, ws._id)

  def getByName(name: String): Option[WebsocketModel] = {
    waspDB.getDocumentByField[WebsocketModel]("name", new BsonString(name)).map(ws => {
      factory(ws)
    })
  }

  def getById(id: String): Option[WebsocketModel] = {
    waspDB.getDocumentByID[WebsocketModel](BsonObjectId(id)).map(ws => {
      factory(ws)
    })
  }

  override def persist(wsModel: WebsocketModel): Unit = waspDB.insert[WebsocketModel](wsModel)
}