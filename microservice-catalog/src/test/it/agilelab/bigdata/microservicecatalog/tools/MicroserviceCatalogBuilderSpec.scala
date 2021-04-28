package it.agilelab.bigdata.microservicecatalog.tools

import java.util.concurrent.CountDownLatch

import com.squareup.okhttp.mockwebserver.{Dispatcher, MockResponse, RecordedRequest}
import com.typesafe.config.ConfigException
import it.agilelab.bigdata.microservicecatalog.MicroserviceCatalogService
import it.agilelab.bigdata.microservicecatalog.entity.{WriteExecutionPlanRequestBody, WriteExecutionPlanResponseBody}
import it.agilelab.bigdata.microservicecatalog.tools.MicroserviceCatalogTestUtils.withServer
import it.agilelab.bigdata.microservicecatalog.tools.builders.microservices.EntitySDK
import it.agilelab.bigdata.microservicecatalog.tools.builders.mockbuilders.{NotExistingServiceBuilder, ParameterBuilder, RightMockBuilder, WrongConfigurationPathBuilder, WrongMicroserviceMockBuilder}
import org.scalatest.FunSuite
import org.scalatest.Matchers.{an, be}



class MicroserviceCatalogBuilderSpec extends FunSuite{
  val ms : Map[String, String]= Map[String, String](("name", "mock"))
  test("Right microservice catalog builder") {
    withServer(dispatcher) { serverData =>
      val microservice: EntitySDK = RightMockBuilder.getMicroserviceCatalogService().getMicroservice(ms)
      val executionPlan: WriteExecutionPlanResponseBody = microservice.getWriteExecutionPlan(WriteExecutionPlanRequestBody(source = "External"))
      assert(executionPlan.writeUri == "s3://bucket/")
      assert(microservice.entityName == "mock")
    }
  }

  test("Wrong microservice class") {
    withServer(dispatcher) { serverData =>
      var entity: EntitySDK = null
      an[ClassCastException] should be thrownBy (
        entity = WrongMicroserviceMockBuilder.getMicroserviceCatalogService().getMicroservice(ms)
      )
    }
  }

  test("Wrong catalog class") {
    withServer(dispatcher) { serverData =>
      var service: MicroserviceCatalogService[_] = null
      an[ConfigException] should be thrownBy (
        service = WrongConfigurationPathBuilder.getMicroserviceCatalogService()
        )
    }
  }

  test("Not existing catalog service class") {
    withServer(dispatcher) { serverData =>
      var service: MicroserviceCatalogService[_] = null
      an[ClassNotFoundException] should be thrownBy (
        service = NotExistingServiceBuilder.getMicroserviceCatalogService()
        )
    }
  }

  test("Try to instantiate service with no base constructor") {
    withServer(dispatcher) { serverData =>
      var service: MicroserviceCatalogService[_] = null
      an[InstantiationException] should be thrownBy (
        service = ParameterBuilder.getMicroserviceCatalogService()
        )
    }
  }
  def dispatcher(latch: CountDownLatch): Dispatcher =
    new Dispatcher {
      override def dispatch(request: RecordedRequest): MockResponse =
        request.getPath match {
          case "/writeExecutionPlan" =>
            latch.countDown()


            val response: MockResponse = new MockResponse().setBody(
              s"""{
                 |    "writeUri": "s3://bucket/"
                 |    "writeType": "Cold",
                 |    "temporaryCredentials": {
                 |        "r": {
                 |            "accessKeyID": "ReadaccessKeyID",
                 |            "secretKey": "ReadsecretKey",
                 |            "sessionToken": "ReadsessionToken"
                 |        },
                 |        "w": {
                 |            "accessKeyID": "WriteaccessKeyID",
                 |            "secretKey": "WritesecretKey",
                 |            "sessionToken": "WritesessionToken"
                 |        }
                 |    },
                 |}""".stripMargin)
            response
          case _ =>
            new MockResponse().setResponseCode(404)
        }
    }
}
