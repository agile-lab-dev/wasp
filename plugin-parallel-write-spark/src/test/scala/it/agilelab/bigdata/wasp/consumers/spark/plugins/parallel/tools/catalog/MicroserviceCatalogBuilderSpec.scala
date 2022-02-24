package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.catalog

import com.squareup.okhttp.mockwebserver.{Dispatcher, MockResponse, RecordedRequest}
import com.typesafe.config.ConfigException
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.{CatalogCoordinates, MicroserviceCatalogService}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity._
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.catalog.builders.mockbuilders._
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils.ParallelWriteTestUtils.withServer
import org.scalatest.FunSuite
import org.scalatest.Matchers.{an, be}

import java.util.concurrent.CountDownLatch



class MicroserviceCatalogBuilderSpec extends FunSuite{
  val ms : CatalogCoordinates = CatalogCoordinates("msnoop", "mock", "v1")
  test("Right microservice catalog builder") {
    withServer(dispatcher) { serverData =>
      val microservice: ParallelWriteEntity = RightMockBuilder.getMicroserviceCatalogService().getMicroservice(ms)
      val executionPlan: WriteExecutionPlanResponseBody = microservice.getWriteExecutionPlan(WriteExecutionPlanRequestBody(source = "External"))
      assert(ParallelWriteFormat.withName(executionPlan.format) == ParallelWriteFormat.delta)
      assert(executionPlan.writeUri == "s3://bucket/")
      assert(microservice.baseUrl.toString == "http://localhost:9999")
    }
  }


  test("Wrong catalog class") {
    withServer(dispatcher) { serverData =>
      var service: MicroserviceCatalogService = null
      an[ConfigException] should be thrownBy {
        service = WrongConfigurationPathBuilder.getMicroserviceCatalogService()
      }
    }
  }

  test("Not existing catalog service class") {
    withServer(dispatcher) { serverData =>
      var service: MicroserviceCatalogService = null
      an[ClassNotFoundException] should be thrownBy {
        service = NotExistingServiceBuilder.getMicroserviceCatalogService()
      }
    }
  }

  test("Try to instantiate service with no base constructor") {
    withServer(dispatcher) { serverData =>
      var service: MicroserviceCatalogService = null
      an[InstantiationException] should be thrownBy {
        service = ParameterBuilder.getMicroserviceCatalogService()
      }
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
                 |    "format": "Delta",
                 |    "writeUri": "s3://bucket/",
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
                 |    }
                 |}""".stripMargin)
            response
          case _ =>
            new MockResponse().setResponseCode(404)
        }
    }
}
