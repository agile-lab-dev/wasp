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
import it.agilelab.bigdata.wasp.core.messages.RestRequest
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import scala.concurrent.Future
import spray.json._
import NifiRquestJsonProtocol._

/**
  * NiFi Producer.
  *
  * @author Alessandro Marino
  */

class NifiProducerGuardian(env: {val producerBL: ProducerBL; val mlModelBL: MlModelBL}, producerId: String)
  extends Actor
    with Logging {

  implicit val materializer: ActorMaterializer = ActorMaterializer()(WaspSystem.actorSystem)
  val nifiProducerConf: Option[ProducerModel] = env.producerBL.getById(producerId)

  override def receive: Actor.Receive = {

    case RestRequest(httpMethod, data, mlModelId) =>

      val action = data.asJsObject.fields("action").convertTo[String]
      val request = checkActionType(action, data, mlModelId)

      printConf()

      if(nifiProducerConf.isDefined) {
        val uri = getUriFromConfiguration(nifiProducerConf.get)
        httpRequest(uri, request, httpMethod)
        sender() ! true
      }
  }

  def checkActionType(action: String, data: JsValue, mlModelId: String): JsValue = {

    val conf = data.convertTo[NifiRequest]

    action.toUpperCase() match {
      case "UPDATE" =>
        val mlModel = env.mlModelBL.getById(mlModelId)
        if (mlModel.isDefined) {
          val modelFile = Some(WaspDB.getDB.getFileByID(mlModel.get.modelFileId.get))
          NifiRequest(action, conf.id, conf.child, modelFile).toJson
        }
        else {
          logger.error(s"mlModelId field is undefined.")
          NifiRequest(action, conf.id, conf.child, None).toJson // TODO gestire eccezione lato controller (Producer_C)
        }
      case _ => NifiRequest(action, conf.id, conf.child, None).toJson
    }
  }

  def printConf(): Unit = {
    val request = nifiProducerConf.get.configuration.get
    print(request)
  }

  def getUriFromConfiguration(nifiProducerConf: ProducerModel): Uri = {
    val info = nifiProducerConf.configuration.get.parseJson
      .convertTo[NifiProducerConfiguration].request

    Uri.from(scheme = info.scheme, host = info.host, port = info.port)
  }

  def getChildFromConfiguration(nifiProducerConf: ProducerModel): List[NifiPlatform] = {
    nifiProducerConf.configuration.get.parseJson
      .convertTo[NifiProducerConfiguration].child.get
  }

  def httpRequest(uri: Uri, jsonReq: JsValue, httpMethod: HttpMethod): Future[HttpResponse] = {
    Http().singleRequest(
      HttpRequest(uri = uri)
        .withMethod(httpMethod)
        .withEntity(ContentTypes.`application/json`, jsonReq.toString())
    )
  }
}