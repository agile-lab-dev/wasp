package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.AngularOkResponse
import it.agilelab.bigdata.wasp.models.{PipegraphModel, StrategyModel}
import it.agilelab.bigdata.wasp.models.editor.{ErrorDTO, NifiStatelessInstanceModel, PipegraphDTO, ProcessGroupResponse}
import it.agilelab.bigdata.wasp.utils.JsonSupport
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

  class MockPipegraphEditorService extends PipegraphEditorService {
    override def checkIOByName(name: String, datastore: DatastoreProduct): Option[ErrorDTO] = {
      val ios = List("IO_1", "IO_2")
      if (ios.contains(name)) Some(ErrorDTO.alreadyExists("Streaming IO", name)) else None
    }

    override def checkPipegraphName(name: String): Option[ErrorDTO] = {
      val pipegraphs = List("pipegraph_1", "pipegraph_2")
      if (pipegraphs.contains(name)) Some(ErrorDTO.alreadyExists("Pipegraph", name)) else None
    }

    override def insertPipegraphModel(model: PipegraphModel): Unit = {}

    override def checkStrategy(strategy: StrategyModel): List[ErrorDTO] = List.empty
  }

  val editroService          = new MockEditorService
  val pipegraphEditorService = new MockPipegraphEditorService
  val controller       = new EditorController(editroService, pipegraphEditorService)
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

  // Tests for /editor/pipegraph
  it should "Respond a OK on post empty Pipegraph request" in {
    val testDTO = PipegraphDTO("empty", "description", Some("owner"), List.empty)
    val commitEditorRequest = Post(s"/editor/pipegraph", testDTO)

    commitEditorRequest ~> controller.getRoutes ~> check {
      val response: AngularResponse[String] = responseAs[AngularResponse[String]]
      response.Result shouldBe "OK"
    }
  }

  it should "Respond a KO on already existing Pipegraph request" in {
    val testDTO = PipegraphDTO("pipegraph_1", "description", Some("owner"), List.empty)
    val commitEditorRequest = Post(s"/editor/pipegraph", testDTO)

    commitEditorRequest ~> controller.getRoutes ~> check {
      val response: AngularResponse[List[ErrorDTO]] = responseAs[AngularResponse[List[ErrorDTO]]]

      response.data shouldBe List(ErrorDTO.alreadyExists("Pipegraph", "pipegraph_1"))
      response.Result shouldBe "KO"
    }
  }

}
