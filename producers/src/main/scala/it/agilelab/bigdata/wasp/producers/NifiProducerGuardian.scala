package it.agilelab.bigdata.wasp.producers

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import it.agilelab.bigdata.wasp.core.WaspSystem.actorSystem
import it.agilelab.bigdata.wasp.core.bl.{MlModelBL, ProducerBL}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.ProducerModel
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.messages.{RestRequest, Start, Stop}
import it.agilelab.bigdata.wasp.core.utils.WaspDB

import scala.concurrent.Future
import spray.json._
import NifiRquestJsonProtocol._

class NifiProducerGuardian(env: {val producerBL: ProducerBL; val mlModelBL: MlModelBL}, producerId: String)
  extends Actor
    with Logging {

  val name: String = "NifiProducerGuardian"
  implicit val materializer = ActorMaterializer()(WaspSystem.actorSystem)

  val nifiProducerConf: Option[ProducerModel] = env.producerBL.getById(producerId)
  val configuration = getConfiguration(nifiProducerConf)
  val url = s"http://localhost:1080"

  override def receive: Actor.Receive = {

    case RestRequest(httpMethod, data, mlModelId) =>
      val action = data.asJsObject.fields("action").convertTo[String]
      val request = checkActionType(action, data, mlModelId)
      if(nifiProducerConf.isDefined) {
        get(url, request, httpMethod)
        sender() ! true
      }
  }

  // Get the content of the configuration field from MongoDB
  def getConfiguration(producerConf: Option[ProducerModel]): Option[String] = {
    if (!producerConf.isDefined) None
    val conf = producerConf.get.configuration
    logger.info(s"Configuration $conf")
    conf
  }

  def checkActionType(action: String, data: JsValue, mlModelId: String): JsValue = {

    val conf = data.convertTo[NifiRequest]

    action.toUpperCase() match {
      case "UPDATE" => {
        val mlModel = env.mlModelBL.getById(mlModelId)
        if (mlModel.isDefined) {
          val modelFile = Some(WaspDB.getDB.getFileByID(mlModel.get.modelFileId.get))
          NifiRequest(action, conf.id, conf.child, modelFile).toJson
        }
        else {
          logger.error(s"mlModelId field is undefined.")
          NifiRequest(action, conf.id, conf.child, None).toJson // TODO gestire eccezione lato controller (Producer_C)
        }
      }
      case _ => NifiRequest(action, conf.id, conf.child, None).toJson
    }
  }

  // Get Future[HttpResponse] representing http-response
  def get(url: String, jsonReq: JsValue, httpMethod: HttpMethod): Future[HttpResponse] = {
    Http().singleRequest(
      HttpRequest(uri = url)
        .withMethod(httpMethod)
        .withEntity(ContentTypes.`application/json`, jsonReq.toString())
    )
  }
}