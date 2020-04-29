package it.agilelab.bigdata.wasp.master.web.controllers

import java.time.Instant

import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import it.agilelab.bigdata.wasp.core.models
import it.agilelab.bigdata.wasp.core.models.LogEntry
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsonFormat, RootJsonFormat}
import scala.concurrent.duration._
import scala.concurrent.Future
import akka.testkit.TestDuration

class MockLogsService extends LogsService {

  val data = Seq(
    LogEntry(
      "it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.telemetry.TelemetryActorKafkaProducer$",
      "Info",
      "Telemetry configuration\nKafkaEntryConfig(batch.size,1048576)\nKafkaEntryConfig(acks,0)",
      Instant.parse("2020-04-28T13:32:00.98Z"),
      "WASP-akka.actor.default-dispatcher-3"
    ),
    models.LogEntry(
      "it.agilelab.bigdata.wasp.core.utils.WaspDBImp",
      "Info",
      "Locating document(s) by key name with value BsonString{value='LoggerPipegraph'} on collection pipegraphs",
      Instant.parse("2020-04-28T13:35:10.198Z"),
      "WASP-akka.actor.default-dispatcher-5"
    ),
    models.LogEntry(
      "it.agilelab.bigdata.wasp.master.MasterGuardian",
      "Info",
      "Starting pipegraph 'LoggerPipegraph'",
      Instant.parse("2020-04-28T13:35:10.273Z"),
      "WASP-akka.actor.default-dispatcher-5"
    ),
    models.LogEntry(
      "it.agilelab.bigdata.wasp.master.MasterGuardianExpected",
      "Info",
      "Call invocation: message: StartPipegraph(LoggerPipegraph) result: Left(Pipegraph 'LoggerPipegraph' start not accepted due to [Cannot start more than one instance of [LoggerPipegraph]])",
      Instant.parse("2020-04-28T13:35:10.453Z"),
      "WASP-akka.actor.default-dispatcher-5"
    )
  )

  override def logs(search: String,
                    startTimestamp: Instant,
                    endTimestamp: Instant,
                    page: Int,
                    size: Int): Future[Seq[LogEntry]] = {

    Future.successful(
      data
        .filter(x => x.message.contains(search))
        .filter(x => x.timestamp.isAfter(startTimestamp))
        .filter(x => x.timestamp.isBefore(endTimestamp))
        .take(size)
    )

  }
}

class LogsControllerSpec
    extends FlatSpec
    with ScalatestRouteTest
    with Matchers
    with JsonSupport {
  implicit def angularResponse[T: JsonFormat]
    : RootJsonFormat[AngularResponse[T]] = jsonFormat2(AngularResponse.apply[T])

  implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)

  it should "Respond to get requests" in {
    val service = new MockLogsService
    val controller = new LogsController(service)

    Get(
      s"/logs?search=Call&startTimestamp=2020-04-28T13:35:10.273Z&endTimestamp=${Instant.now().toString}&page=0&size=100"
    ) ~> controller.logs(false) ~> check {
      val response = responseAs[AngularResponse[Seq[LogEntry]]]

      response.data.length should be(1)
      response.data.head.log_source should be("it.agilelab.bigdata.wasp.master.MasterGuardianExpected")
    }

  }

}
