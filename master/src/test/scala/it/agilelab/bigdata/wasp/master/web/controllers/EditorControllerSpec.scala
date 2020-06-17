package it.agilelab.bigdata.wasp.master.web.controllers

import java.util.UUID
import java.util.concurrent.Executors

import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.stream.scaladsl.Source
import akka.util.ByteString
import it.agilelab.bigdata.nifi.client.core.SttpSerializer
import it.agilelab.bigdata.nifi.client.{NifiClient, NifiRawClient}
import it.agilelab.bigdata.wasp.core.models.editor.NifiStatelessInstanceModel
import it.agilelab.bigdata.wasp.master.web.controllerseditorInstanceFormat.NifiEditorService
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.AngularOkResponse
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import org.scalatest.{FlatSpec, Matchers}
import spray.json._
import sttp.client.akkahttp.Types.LambdaFlow
import sttp.client.monad.{FutureMonad, MonadError}
import sttp.client.ws.WebSocketResponse
import sttp.client.{Request, Response, SttpBackend}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}


class EditorControllerSpec extends FlatSpec with ScalatestRouteTest with Matchers with JsonSupport {

  class MockEditorService(nifiClient: NifiClient[Future]) extends NifiEditorService(nifiClient) {
    override def newEditorSession(processGroupName: String) = {
      Future(NifiStatelessInstanceModel(processGroupName, "www.test.com/" + processGroupName, "1234"))
    }
  }

  implicit def angularResponse[T: JsonFormat]: RootJsonFormat[AngularResponse[T]] =
    jsonFormat2(AngularResponse.apply[T])
  implicit val timeout: RouteTestTimeout = RouteTestTimeout(FiniteDuration(10, "seconds"))

  "EditorController" should "Respond to post requests" in {

    val clientExecutionContext: ExecutionContextExecutor =
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(4))

    implicit val akkaBackend: SttpBackend[Future, Source[ByteString, Any], LambdaFlow] =
      new SttpBackend[Future, Source[ByteString, Any], LambdaFlow] {
        override def send[T](request: Request[T, Source[ByteString, Any]]): Future[Response[T]] = ???

        override def openWebsocket[T, WS_RESULT](
            request: Request[T, Source[ByteString, Any]],
            handler: LambdaFlow[WS_RESULT]
        ): Future[WebSocketResponse[WS_RESULT]] = ???

        override def close(): Future[Unit] = ???

        override def responseMonad: MonadError[Future] = ???
      }

    implicit val monadForFuture: FutureMonad = new FutureMonad()(clientExecutionContext)
    implicit val serializer: SttpSerializer  = new SttpSerializer()

    val nifiClient = new NifiClient(NifiRawClient("test", "test"), UUID.randomUUID())

    val service          = new MockEditorService(nifiClient)
    val controller       = new EditorController(service)
    val processGroupName = "testName"

    val expectedResponse = new AngularOkResponse(
      NifiStatelessInstanceModel(processGroupName, "www.test.com/" + processGroupName, "1234").toJson
    ).toAngularOkResponse(false)

    val request = Post(
      s"/editor/nifi/$processGroupName"
    )

    request ~> controller.getRoute ~> check {
      val response = responseAs[AngularResponse[NifiStatelessInstanceModel]]
      response.data.url shouldBe "www.test.com/" + processGroupName
      response.data.name shouldBe processGroupName
    }
  }

}
