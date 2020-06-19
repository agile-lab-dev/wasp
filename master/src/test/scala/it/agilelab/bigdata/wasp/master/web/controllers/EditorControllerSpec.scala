package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import it.agilelab.bigdata.wasp.core.models.editor.NifiStatelessInstanceModel
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration


class EditorControllerSpec extends FlatSpec with ScalatestRouteTest with Matchers with JsonSupport {

  class MockEditorService() extends EditorService {
    override def newEditorSession(processGroupName: String) = {
      Future(NifiStatelessInstanceModel(processGroupName, "www.test.com/" + processGroupName, "1234"))
    }
  }

  implicit def angularResponse[T: JsonFormat]: RootJsonFormat[AngularResponse[T]] =
    jsonFormat2(AngularResponse.apply[T])
  implicit val timeout: RouteTestTimeout = RouteTestTimeout(FiniteDuration(10, "seconds"))

  "EditorController" should "Respond to post requests" in {

    val service          = new MockEditorService()
    val controller       = new EditorController(service)
    val processGroupName = "testName"

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
