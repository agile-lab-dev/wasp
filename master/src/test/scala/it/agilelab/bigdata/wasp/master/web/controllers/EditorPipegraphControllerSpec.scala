package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct}
import it.agilelab.bigdata.wasp.models.{
  DatastoreModel,
  FreeCodeModel,
  IndexModel,
  KeyValueModel,
  PipegraphModel,
  ProcessGroupModel,
  RawModel,
  StrategyModel,
  TopicModel
}
import it.agilelab.bigdata.wasp.models.editor.{
  ErrorDTO,
  FlowNifiDTO,
  NifiStatelessInstanceModel,
  PipegraphDTO,
  ProcessGroupResponse
}
import it.agilelab.bigdata.wasp.utils.JsonSupport
import org.json4s.JsonAST.{JObject}
import org.scalatest.{FlatSpec, Matchers}
import spray.json._

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration

/**
  * Tests for Pipegraph Editor API (/editor/pipegraph)
  */
class EditorPipegraphControllerSpec extends FlatSpec with ScalatestRouteTest with Matchers with JsonSupport {

  implicit def angularResponse[T: JsonFormat]: RootJsonFormat[AngularResponse[T]] =
    jsonFormat2(AngularResponse.apply[T])
  implicit val timeout: RouteTestTimeout = RouteTestTimeout(FiniteDuration(10, "seconds"))

  class MockEditorService extends EditorService {
    override def newEditorSession(processGroupName: String): Future[NifiStatelessInstanceModel] =
      Future.successful(NifiStatelessInstanceModel(processGroupName, "", ""))
    override def commitEditorSession(processGroupId: String): Future[ProcessGroupResponse] =
      Future.successful(ProcessGroupResponse(id = processGroupId, content = JObject(List.empty)))
  }

  class MockPipegraphEditorService extends EmptyPipegraphEditorService {
    override def getAllUIPipegraphs: List[PipegraphModel] = {
      val pipegraph = PipegraphModel(
        "pipegraph_base",
        "description",
        "UI",
        false,
        System.currentTimeMillis(),
        List.empty,
        None
      )
      List(pipegraph)
    }
    override def checkIOByName(name: String, datastore: DatastoreProduct): Option[ErrorDTO] = {
      val ios = List("IO_1", "IO_2")
      if (ios.contains(name)) Some(ErrorDTO.alreadyExists("Streaming IO", name)) else None
    }
    override def checkPipegraphName(name: String, isUpdate: Boolean): Option[ErrorDTO] = {
      val pipegraphs = List("pipegraph_1", "pipegraph_2")
      (pipegraphs.contains(name), isUpdate) match {
        case (true, false) => Some(ErrorDTO.alreadyExists("Pipegraph", name))
        case (false, true) => Some(ErrorDTO.notFound("Pipegraph", name))
        case _             => None
      }
    }
    override def insertPipegraphModel(model: PipegraphModel): Unit = {}
    override def updatePipegraphModel(model: PipegraphModel): Unit = {}

    override def getUIPipegraph(name: String): Option[PipegraphModel] = {
      val json =
        """{
          |  "name": "test",
          |  "description": "",
          |  "owner": "",
          |  "structuredStreamingComponents": [
          |    {
          |      "name": "etl_event-topic.topic_My Free Code_logger_solr_index",
          |      "triggerIntervalMs": 0,
          |      "streamingInput": {
          |        "datastoreModel": {
          |          "name": "event-topic.topic",
          |          "modelType": "topic"
          |        },
          |        "name": "reader_event-topic.topic",
          |        "options": {}
          |      },
          |      "strategy": {
          |        "strategyType": "freecode",
          |        "className": "",
          |        "name": "My Free Code",
          |        "code": "dataFrames.head._2",
          |        "processGroup": ""
          |      },
          |      "streamingOutput": {
          |        "name": "writer_logger_solr_index",
          |        "datastoreModel": {
          |          "modelType": "index",
          |          "name": "logger_solr_index"
          |        },
          |        "options": {}
          |      },
          |      "group": "",
          |      "options": {}
          |    }
          |  ]
          |}""".stripMargin

      lazy val data: PipegraphDTO = implicitly[RootJsonFormat[PipegraphDTO]].read(spray.json.JsonParser(json))

      this
        .toPipegraphModel(data)
        .fold(x => {
          None
        }, Some(_))
    }

    override def parsePGJson(json: String): Option[(String, String, JsValue)] = {
      import spray.json._
      val parsedJson = json.parseJson

      val id: Option[String]       = parsedJson.asJsObject.fields.get("id").map(_.convertTo[String])
      val content: Option[JsValue] = parsedJson.asJsObject.fields.get("content")

      val variables: Option[JsValue] =
        content.flatMap(y => y.asJsObject.fields.get("variables"))
      val errorPort: Option[String] = content.flatMap(y =>
        y.asJsObject.fields
          .get("outputPorts")
          .map(ports => ports.convertTo[List[Map[String, JsValue]]])
          .flatMap(
            _.filter(n => n.getOrElse("name", JsString.empty) == JsString("wasp-error")).head.get("identifier")
          )
          .map(_.convertTo[String])
      )

      for (a <- id; b <- errorPort; c <- variables) yield (a, b, c)
    }
  }

  val editroService          = new MockEditorService
  val pipegraphEditorService = new MockPipegraphEditorService
  val controller             = new EditorController(editroService, pipegraphEditorService)

  it should "json should be parsable" in {
    val json =
      """
      |{
      |  "name": "roudata lorenzo pipegraph",
      |  "description": "jojo",
      |  "owner": "ui",
      |  "structuredStreamingComponents": [
      |    {
      |      "name": "etl_event_topics_huhu_mainiuroudata",
      |      "triggerIntervalMs": 80000,
      |      "streamingInput": {
      |        "datastoreModel": {
      |          "name": "event_topics",
      |          "modelType": "topic"
      |        },
      |        "name": "reader_event_topics",
      |        "options": {}
      |      },
      |      "strategy": {
      |        "strategyType": "freecode",
      |        "className": "",
      |        "name": "huhu",
      |        "code": "\n dataFrames.head._2\n",
      |        "processGroup": "",
      |        "config": {}
      |      },
      |      "streamingOutput": {
      |        "name": "writer_mainiuroudata",
      |        "datastoreModel": {
      |          "name": "mainiuroudata",
      |          "modelType": "rawdata",
      |          "config": {
      |            "name": "mainiuroudata",
      |            "uri": "/lorenzo/test",
      |            "timed": false,
      |            "options": {
      |              "format": "parquet",
      |              "saveMode": "append",
      |              "extraOptions": {},
      |              "partitionBy": []
      |            },
      |            "schema": "{}"
      |          }
      |        },
      |        "options": {}
      |      },
      |      "group": "",
      |      "options": {}
      |    }
      |  ]
      |}
      |""".stripMargin

    lazy val data: PipegraphDTO = implicitly[RootJsonFormat[PipegraphDTO]].read(spray.json.JsonParser(json))
  }

  it should "Parse the following json as PipegraphDTO" in {
    val json =
      """{
        |  "name": "test",
        |  "description": "",
        |  "owner": "",
        |  "structuredStreamingComponents": [
        |    {
        |      "name": "etl_event-topic.topic_My Free Code_logger_solr_index",
        |      "triggerIntervalMs": 0,
        |      "streamingInput": {
        |        "datastoreModel": {
        |          "name": "event-topic.topic",
        |          "modelType": "topic"
        |        },
        |        "name": "reader_event-topic.topic",
        |        "options": {}
        |      },
        |      "strategy": {
        |        "strategyType": "freecode",
        |        "className": "",
        |        "name": "My Free Code",
        |        "code": "dataFrames.head._2",
        |        "processGroup": ""
        |      },
        |      "streamingOutput": {
        |        "name": "writer_logger_solr_index",
        |        "datastoreModel": {
        |          "modelType": "index",
        |          "name": "logger_solr_index"
        |        },
        |        "options": {}
        |      },
        |      "group": "",
        |      "options": {}
        |    },
        |    {
        |      "name": "etl_event-topic.topic_My NiFI Code_multitopic_plaintext",
        |      "triggerIntervalMs": 2000,
        |      "streamingInput": {
        |        "datastoreModel": {
        |          "name": "event-topic.topic",
        |          "modelType": "topic"
        |        },
        |        "name": "reader_event-topic.topic",
        |        "options": {}
        |      },
        |      "strategy": {
        |        "strategyType": "nifi",
        |        "className": "",
        |        "name": "My NiFI Code",
        |        "code": "",
        |        "processGroup": "{\"id\":\"9fc6319e-0173-1000-108a-3348c63ad4a0\",\"content\":{\"name\":\"6e8c1cdd-b221-4047-a8f9-eee4054a58fa\",\"remoteProcessGroups\":[],\"identifier\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"processors\":[{\"penaltyDuration\":\"30 sec\",\"name\":\"AttributesToJSON\",\"bundle\":{\"group\":\"org.apache.nifi\",\"artifact\":\"nifi-standard-nar\",\"version\":\"1.11.4\"},\"identifier\":\"d9fb7f87-1133-3d64-a73c-81a8db60950d\",\"style\":{},\"yieldDuration\":\"1 sec\",\"runDurationMillis\":0,\"propertyDescriptors\":{\"attributes-to-json-regex\":{\"name\":\"attributes-to-json-regex\",\"displayName\":\"Attributes Regular Expression\",\"identifiesControllerService\":false,\"sensitive\":false},\"Include Core Attributes\":{\"name\":\"Include Core Attributes\",\"displayName\":\"Include Core Attributes\",\"identifiesControllerService\":false,\"sensitive\":false},\"Attributes List\":{\"name\":\"Attributes List\",\"displayName\":\"Attributes List\",\"identifiesControllerService\":false,\"sensitive\":false},\"Null Value\":{\"name\":\"Null Value\",\"displayName\":\"Null Value\",\"identifiesControllerService\":false,\"sensitive\":false},\"Destination\":{\"name\":\"Destination\",\"displayName\":\"Destination\",\"identifiesControllerService\":false,\"sensitive\":false}},\"concurrentlySchedulableTaskCount\":1,\"executionNode\":\"ALL\",\"componentType\":\"PROCESSOR\",\"properties\":{\"Destination\":\"flowfile-attribute\",\"Null Value\":\"false\",\"Include Core Attributes\":\"true\"},\"schedulingPeriod\":\"0 sec\",\"autoTerminatedRelationships\":[],\"bulletinLevel\":\"WARN\",\"groupIdentifier\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"position\":{\"x\":1008,\"y\":144},\"type\":\"org.apache.nifi.processors.standard.AttributesToJSON\",\"comments\":\"\",\"scheduledState\":\"ENABLED\",\"schedulingStrategy\":\"TIMER_DRIVEN\"}],\"processGroups\":[],\"funnels\":[],\"variables\":{},\"componentType\":\"PROCESS_GROUP\",\"labels\":[],\"position\":{\"x\":0,\"y\":0},\"outputPorts\":[{\"name\":\"wasp-output\",\"identifier\":\"a747be08-e9b8-39d4-bb93-338f3051eb30\",\"concurrentlySchedulableTaskCount\":1,\"componentType\":\"OUTPUT_PORT\",\"allowRemoteAccess\":false,\"groupIdentifier\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"position\":{\"x\":1600,\"y\":0},\"type\":\"OUTPUT_PORT\"},{\"name\":\"wasp-error\",\"identifier\":\"644066ac-fb4c-3281-8bdc-666487d8aa51\",\"concurrentlySchedulableTaskCount\":1,\"componentType\":\"OUTPUT_PORT\",\"allowRemoteAccess\":false,\"groupIdentifier\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"position\":{\"x\":1688,\"y\":224},\"type\":\"OUTPUT_PORT\"}],\"controllerServices\":[],\"connections\":[{\"name\":\"\",\"source\":{\"id\":\"15dfe6a0-81a8-3675-b2c0-3c9ced4a1806\",\"type\":\"INPUT_PORT\",\"groupId\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"name\":\"wasp-input\"},\"flowFileExpiration\":\"0 sec\",\"backPressureObjectThreshold\":10000,\"identifier\":\"ba02094d-a4a7-39c7-98aa-fec022fe4446\",\"labelIndex\":1,\"loadBalanceStrategy\":\"DO_NOT_LOAD_BALANCE\",\"loadBalanceCompression\":\"DO_NOT_COMPRESS\",\"partitioningAttribute\":\"\",\"backPressureDataSizeThreshold\":\"1 GB\",\"bends\":[],\"componentType\":\"CONNECTION\",\"prioritizers\":[],\"groupIdentifier\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"zIndex\":0,\"destination\":{\"name\":\"AttributesToJSON\",\"groupId\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"id\":\"d9fb7f87-1133-3d64-a73c-81a8db60950d\",\"type\":\"PROCESSOR\",\"comments\":\"\"},\"selectedRelationships\":[\"\"]},{\"name\":\"\",\"source\":{\"name\":\"AttributesToJSON\",\"groupId\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"id\":\"d9fb7f87-1133-3d64-a73c-81a8db60950d\",\"type\":\"PROCESSOR\",\"comments\":\"\"},\"flowFileExpiration\":\"0 sec\",\"backPressureObjectThreshold\":10000,\"identifier\":\"c6c371dc-0d0d-30f5-b2c9-cc175d3f5892\",\"labelIndex\":1,\"loadBalanceStrategy\":\"DO_NOT_LOAD_BALANCE\",\"loadBalanceCompression\":\"DO_NOT_COMPRESS\",\"partitioningAttribute\":\"\",\"backPressureDataSizeThreshold\":\"1 GB\",\"bends\":[],\"componentType\":\"CONNECTION\",\"prioritizers\":[],\"groupIdentifier\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"zIndex\":0,\"destination\":{\"id\":\"a747be08-e9b8-39d4-bb93-338f3051eb30\",\"type\":\"OUTPUT_PORT\",\"groupId\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"name\":\"wasp-output\"},\"selectedRelationships\":[\"success\"]},{\"name\":\"\",\"source\":{\"name\":\"AttributesToJSON\",\"groupId\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"id\":\"d9fb7f87-1133-3d64-a73c-81a8db60950d\",\"type\":\"PROCESSOR\",\"comments\":\"\"},\"flowFileExpiration\":\"0 sec\",\"backPressureObjectThreshold\":10000,\"identifier\":\"7eebb41b-5f5c-3dd0-817c-b16342cbdd03\",\"labelIndex\":1,\"loadBalanceStrategy\":\"DO_NOT_LOAD_BALANCE\",\"loadBalanceCompression\":\"DO_NOT_COMPRESS\",\"partitioningAttribute\":\"\",\"backPressureDataSizeThreshold\":\"1 GB\",\"bends\":[],\"componentType\":\"CONNECTION\",\"prioritizers\":[],\"groupIdentifier\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"zIndex\":0,\"destination\":{\"id\":\"644066ac-fb4c-3281-8bdc-666487d8aa51\",\"type\":\"OUTPUT_PORT\",\"groupId\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"name\":\"wasp-error\"},\"selectedRelationships\":[\"failure\"]}],\"comments\":\"\",\"inputPorts\":[{\"name\":\"wasp-input\",\"identifier\":\"15dfe6a0-81a8-3675-b2c0-3c9ced4a1806\",\"concurrentlySchedulableTaskCount\":1,\"componentType\":\"INPUT_PORT\",\"allowRemoteAccess\":false,\"groupIdentifier\":\"eee92abe-170c-32f6-92f0-62805b42e369\",\"position\":{\"x\":700,\"y\":0},\"type\":\"INPUT_PORT\"}]}}"
        |      },
        |      "streamingOutput": {
        |        "name": "writer_multitopic_plaintext",
        |        "datastoreModel": {
        |          "modelType": "topic",
        |          "name": "multitopic_plaintext"
        |        },
        |        "options": {}
        |      },
        |      "group": "",
        |      "options": {}
        |    }
        |  ]
        |}""".stripMargin

    lazy val data: PipegraphDTO = implicitly[RootJsonFormat[PipegraphDTO]].read(spray.json.JsonParser(json))

    val strategyJson: String = data.structuredStreamingComponents
      .filter(_.name == "etl_event-topic.topic_My NiFI Code_multitopic_plaintext")
      .head
      .strategy
      .asInstanceOf[FlowNifiDTO]
      .processGroup

    lazy val pipegraph: Either[List[ErrorDTO], PipegraphModel] = pipegraphEditorService.toPipegraphModel(data)

    pipegraph shouldBe a [Left[List[ErrorDTO], PipegraphModel]]
    data shouldBe a [PipegraphDTO]
  }

  it should "merge configs" in {
    val str1 = s"""
       | nifi.process-group-id = "1"
       | nifi.error-port = "23"
       | nifi.variables = {}""".stripMargin

    val str2 = s"""
                  | name = "Peppo"
                  | nifi.variables = {
                  |   test1 = 123123
                  |   something = "hello"
                  | }""".stripMargin

    val res: String = pipegraphEditorService.mergeConfigsStrings(str1, str2)

    res shouldBe "{\"name\":\"Peppo\",\"nifi\":{\"error-port\":\"23\",\"process-group-id\":\"1\",\"variables\":{\"something\":\"hello\",\"test1\":123123}}}"
  }

  it should "parse NIFI document" in {
    import spray.json._
    val json = """{
                 |      "id":"6c919184-0173-1000-257c-b93f600c0774",
                 |      "content":{
                 |         "name":"namenwepg",
                 |         "remoteProcessGroups":[],
                 |         "identifier":"72473e36-6d17-3425-be11-d86170da8939",
                 |         "processors":[],
                 |         "processGroups":[],
                 |         "funnels":[],
                 |         "variables":{},
                 |         "componentType":"PROCESS_GROUP",
                 |         "labels":[],
                 |         "position":{
                 |            "x":304.0,
                 |            "y":48.0
                 |         },
                 |         "outputPorts":[
                 |            {
                 |               "name":"wasp-error",
                 |               "identifier":"f4332488-ae60-3d14-a5bf-8a5065090aea",
                 |               "concurrentlySchedulableTaskCount":1,
                 |               "componentType":"OUTPUT_PORT",
                 |               "allowRemoteAccess":false,
                 |               "groupIdentifier":"72473e36-6d17-3425-be11-d86170da8939",
                 |               "position":{
                 |                  "x":1000.0,
                 |                  "y":100.0
                 |               },
                 |               "type":"OUTPUT_PORT"
                 |            },
                 |            {
                 |               "name":"wasp-output",
                 |               "identifier":"e4830fc7-f602-38f5-90d3-b1fa44bcc112",
                 |               "concurrentlySchedulableTaskCount":1,
                 |               "componentType":"OUTPUT_PORT",
                 |               "allowRemoteAccess":false,
                 |               "groupIdentifier":"72473e36-6d17-3425-be11-d86170da8939",
                 |               "position":{
                 |                  "x":1000.0,
                 |                  "y":0.0
                 |               },
                 |               "type":"OUTPUT_PORT"
                 |            }
                 |         ],
                 |         "controllerServices":[],
                 |         "connections":[],
                 |         "comments":"",
                 |         "inputPorts":[
                 |            {
                 |               "name":"wasp-input",
                 |               "identifier":"4ac14fb5-cbe3-3e58-bd0a-e8eb9ef65b53",
                 |               "concurrentlySchedulableTaskCount":1,
                 |               "componentType":"INPUT_PORT",
                 |               "allowRemoteAccess":false,
                 |               "groupIdentifier":"72473e36-6d17-3425-be11-d86170da8939",
                 |               "position":{
                 |                  "x":700.0,
                 |                  "y":0.0
                 |               },
                 |               "type":"INPUT_PORT"
                 |            }
                 |         ]
                 |      }
                 |}""".stripMargin

    val result = pipegraphEditorService.parsePGJson(json).get

    result._1 shouldBe "6c919184-0173-1000-257c-b93f600c0774"
    result._2 shouldBe "f4332488-ae60-3d14-a5bf-8a5065090aea"
  }

  /**
    * GET
    */
  it should "Respond a OK on get request" in {
    val commitEditorRequest = Get(s"/editor/pipegraph")

    commitEditorRequest ~> controller.getRoutes ~> check {
      val response: AngularResponse[List[PipegraphDTO]] = responseAs[AngularResponse[List[PipegraphDTO]]]
      response.Result shouldBe "OK"
    }
  }

  it should "Return an error list with missing pipegraph" in {
    val getRequest = Get("/editor/pipegraph/test_missing")

    getRequest ~> controller.getRoutes ~> check {
      val response: AngularResponse[List[ErrorDTO]] = responseAs[AngularResponse[List[ErrorDTO]]]
      response.Result shouldBe "KO"
    }
  }

  /**
    * POST
    */
  it should "Respond a OK on post empty Pipegraph request" in {
    val testDTO             = PipegraphDTO("empty", "description", Some("owner"), List.empty)
    val commitEditorRequest = Post(s"/editor/pipegraph", testDTO)

    commitEditorRequest ~> controller.getRoutes ~> check {
      val response: AngularResponse[String] = responseAs[AngularResponse[String]]
      response.Result shouldBe "OK"
    }
  }

  it should "Respond a KO on already existing Pipegraph request" in {
    val testDTO             = PipegraphDTO("pipegraph_1", "description", Some("owner"), List.empty)
    val commitEditorRequest = Post(s"/editor/pipegraph", testDTO)

    commitEditorRequest ~> controller.getRoutes ~> check {
      val response: AngularResponse[List[ErrorDTO]] = responseAs[AngularResponse[List[ErrorDTO]]]

      response.data shouldBe List(ErrorDTO.alreadyExists("Pipegraph", "pipegraph_1"))
      response.Result shouldBe "KO"
    }
  }

  /**
    * PUT
    */
  it should "Respond a OK on already existing Pipegraph request" in {
    val testDTO             = PipegraphDTO("pipegraph_1", "description", Some("owner"), List.empty)
    val commitEditorRequest = Put(s"/editor/pipegraph", testDTO)

    commitEditorRequest ~> controller.getRoutes ~> check {
      val response: AngularResponse[String] = responseAs[AngularResponse[String]]
      response.Result shouldBe "OK"
    }
  }
}
