package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{BSONConversionHelper, WebsocketModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import reactivemongo.bson.{BSONString, BSONObjectID}
import reactivemongo.api.commands.WriteResult

import scala.concurrent.Future

trait WebsocketBL {
  def getByName(name: String): Future[Option[WebsocketModel]]

  def getById(id: String): Future[Option[WebsocketModel]]

  def persist(topicModel: WebsocketModel): Future[WriteResult]
}


class WebsocketBLImp(waspDB: WaspDB) extends WebsocketBL with BSONConversionHelper {

  import scala.concurrent.ExecutionContext.Implicits.global

  private def factory(ws: WebsocketModel) = new WebsocketModel(ws.name, ws.host, ws.port, ws.resourceName, ws.options, ws._id)

  def getByName(name: String): Future[Option[WebsocketModel]] = {
    waspDB.getDocumentByField[WebsocketModel]("name", new BSONString(name)).map(ws => {
      ws.map(p => factory(p))
    })
  }

  def getById(id: String): Future[Option[WebsocketModel]] = {
    waspDB.getDocumentByID[WebsocketModel](BSONObjectID(id)).map(ws => {
      ws.map(p => factory(p))
    })
  }

  override def persist(wsModel: WebsocketModel): Future[WriteResult] = waspDB.insert[WebsocketModel](wsModel)
}