package it.agilelab.bigdata.wasp.producers

import java.io.File
import java.util.Base64

import akka.actor.Actor
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import it.agilelab.bigdata.wasp.core.WaspSystem.actorSystem
import it.agilelab.bigdata.wasp.repository.core.bl.{ConfigBL, MlModelBL, ProducerBL}
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.messages.{ModelKey, RestRequest}

import scala.concurrent.Future
import spray.json._
import NifiRquestJsonProtocol._
import it.agilelab.bigdata.wasp.models.ProducerModel
import org.apache.commons.io.FileUtils

/**
  * NiFi Producer.
  *
  * @author Alessandro Marino
  */

class NifiProducerGuardian(env: {val producerBL: ProducerBL; val mlModelBL: MlModelBL}, producerName: String)
  extends Actor
    with Logging {

  implicit val materializer: ActorMaterializer = ActorMaterializer()(WaspSystem.actorSystem)
  val nifiProducerConf: Option[ProducerModel] = env.producerBL.getByName(producerName)

  override def receive: Actor.Receive = {

    case RestRequest(httpMethod, data, modelKey) =>

      val action = data.asJsObject.fields("action").convertTo[String]
      val request = checkActionType(action, data, modelKey)

      if (nifiProducerConf.isDefined) {
        val uri = getUriFromConfiguration(nifiProducerConf.get)
        val _ = httpRequest(uri, request, httpMethod)
        sender() ! Right(())
      }
  }

  def checkActionType(action: String, data: JsValue, modelKey: ModelKey): JsValue = {

    val conf = data.convertTo[NifiRequest]

    action.toUpperCase() match {
      case "UPDATE" =>
        val mlModel = env.mlModelBL.getMlModelOnlyInfo(modelKey.name, modelKey.version, modelKey.timestamp)
        if (mlModel.isDefined) {
          val modelFile = ConfigBL.mlModelBL.getFileByID(mlModel.get)
          val encodedModel: Option[String] = Some(Base64.getEncoder().encodeToString(modelFile.get))
          val file = new File("/root/wasp/models/encodedModel")
          FileUtils.writeStringToFile(file, encodedModel.get, "UTF-8")
          NifiRequest(action, conf.id, conf.child, encodedModel).toJson

        }
        else throw new RuntimeException(s"mlModel does not exist.")
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