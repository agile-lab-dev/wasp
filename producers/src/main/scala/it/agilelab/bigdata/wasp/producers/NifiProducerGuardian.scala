package it.agilelab.bigdata.wasp.producers

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import it.agilelab.bigdata.wasp.core.WaspSystem.actorSystem
import it.agilelab.bigdata.wasp.core.bl.{MlModelBL, ProducerBL}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.{MlModelOnlyInfo, ProducerModel}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.messages.{RestProducerRequest, Start, Stop}

import scala.concurrent.Future
import spray.json._
import NifiRquestJsonProtocol._
import it.agilelab.bigdata.wasp.producers.launcher.InsertModelLauncher.waspDB

class NifiProducerGuardian(env: {val producerBL: ProducerBL; val mlModelBL: MlModelBL}, producerId: String)
  extends Actor
    with Logging {

  val name: String = "NifiProducerGuardian"
  implicit val materializer = ActorMaterializer()(WaspSystem.actorSystem)

  val nifiProducerConf: Option[ProducerModel] = env.producerBL.getById(producerId)
  val configuration = getConfiguration(nifiProducerConf)
  val url = s"http://localhost:1080"

  override def receive: Actor.Receive = {

    case Start =>
      logger.info(s"Producer $producerId starting at guardian $self")

      if(nifiProducerConf.isDefined) {
        get(url, getRequest(configuration.get, "RUNNING"))
        sender() ! true
      }

    case Stop =>
      logger.info(s"Producer $producerId stopping")

      if(nifiProducerConf.isDefined) {
        get(url, getRequest(configuration.get, "STOPPED"))
        sender() ! true
      }

    case RestProducerRequest (id, httpMethod, body, model_id) =>
      logger.info(s"Producer $producerId: generic request")

      val mlModelOnlyInfo = env.mlModelBL.getById(model_id)
      if(mlModelOnlyInfo.isDefined) {
        val model = waspDB.getFileByID(mlModelOnlyInfo.get.modelFileId.get)
        get(url, getRequest(configuration.get, "UPDATE", Some(model)))
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

  // Get Future[HttpResponse] representing http-request
  def get(url: String, jsonReq: JsValue): Future[HttpResponse] = {
    val method = HttpMethods.PUT
    Http().singleRequest(
      HttpRequest(uri = url)
        .withMethod(HttpMethods.PUT)
        .withEntity(ContentTypes.`application/json`, jsonReq.toString())
    )
  }

  def getRequest(json: String, action: String, model: Option[Array[Byte]] = None): JsValue = {
    val conf = json.parseJson.convertTo[NifiRequest]
    NifiRequest(action, conf.id, conf.child, model).toJson
  }
}