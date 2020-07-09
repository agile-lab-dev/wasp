package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.AngularOkResponse
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import it.agilelab.bigdata.wasp.models.editor.{NifiStatelessInstanceModel, ProcessGroupResponse}
import org.json4s.JsonAST.{JField, JObject, JString}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

class EditorControllerSpec extends FlatSpec with ScalatestRouteTest with Matchers with JsonSupport {
  implicit def angularResponse[T: JsonFormat]: RootJsonFormat[AngularResponse[T]] =
    jsonFormat2(AngularResponse.apply[T])
  implicit val timeout: RouteTestTimeout = RouteTestTimeout(FiniteDuration(10, "seconds"))

  class MockEditorService extends EditorService {
    override def newEditorSession(processGroupName: String): Future[NifiStatelessInstanceModel] = {
      Future.successful(NifiStatelessInstanceModel(processGroupName, "www.test.com/" + processGroupName, "1234"))
    }

    override def commitEditorSession(processGroupId: String): Future[ProcessGroupResponse] = {
      Future.successful(
        ProcessGroupResponse(id = processGroupId, content = JObject(List(JField("ciccio", JString("pasticcio")))))
      )
    }
  }

  val service          = new MockEditorService
  val controller       = new EditorController(service)
  val processGroupName = "testName"
  val processGroupId   = "id"

  it should "Respond a post newEditorRequest" in {
    val expectedResponse = new AngularOkResponse(
      NifiStatelessInstanceModel(processGroupName, "www.test.com/" + processGroupName, "1234").toJson
    ).toAngularOkResponse(false)
    val newEditorRequest = Post(
      s"/editor/nifi/$processGroupName"
    )
    newEditorRequest ~> controller.getRoutes ~> check {
      val response = responseAs[AngularResponse[NifiStatelessInstanceModel]]
      response.data.url shouldBe "www.test.com/" + processGroupName
      response.data.name shouldBe processGroupName
    }

  }

  it should "Respond a put commitEditorRequest" in {

    val commitEditorRequest = Put(
      s"/editor/nifi/$processGroupId"
    )

    commitEditorRequest ~> controller.getRoutes ~> check {
      val response = responseAs[AngularResponse[ProcessGroupResponse]]
      response.data.id shouldBe processGroupId
      response.data.content shouldBe JObject(List(JField("ciccio", JString("pasticcio"))))
    }
  }

}
