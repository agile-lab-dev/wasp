package it.agilelab.bigdata.wasp.master.web.controllers

import java.time.Instant

import akka.http.scaladsl.testkit.{RouteTestTimeout, ScalatestRouteTest}
import akka.testkit.TestDuration
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import it.agilelab.bigdata.wasp.models.{EventEntry, Events}
import org.scalatest.{FlatSpec, Matchers}
import spray.json.{JsonFormat, RootJsonFormat}

import scala.concurrent.Future
import scala.concurrent.duration._

class MockEventsService extends EventsService with JsonSupport {
  lazy val data: Seq[EventEntry] =
    implicitly[RootJsonFormat[Seq[EventEntry]]].read(spray.json.JsonParser(json))
  lazy val json =
    """[{
    "eventId": "f3cb37be-968d-43da-b54f-d0f8fe57871c",
    "eventRuleName": "HighTemperature",
    "eventType": "TempControl",
    "payload": "{\"kafkaMetadata\":{\"key\":\"LTk4OTkwOTMxMA==\",\"headers\":[],\"topic\":\"fake-data.topic\",\"partition\":0,\"offset\":1577,\"timestamp\":\"2020-05-04T18:18:00.307Z\",\"timestampType\":0},\"name\":\"sensor_45\",\"temperature\":184.77844,\"someLong\":1588616280305,\"someStuff\":\"even\",\"someNumber\":79}",
    "severity": "CRITICAL",
    "source": "streamingSource1",
    "sourceId": "sensor_45",
    "timestamp": "2020-05-04T18:18:01.108Z"
  },
  {
    "eventId": "7fb74696-54e8-45a4-99d6-fe94c87748e8",
    "eventRuleName": "HighTemperature",
    "eventType": "TempControl",
    "payload": "{\"kafkaMetadata\":{\"key\":\"MTI3MzA5NjY4NA==\",\"headers\":[],\"topic\":\"fake-data.topic\",\"partition\":1,\"offset\":1574,\"timestamp\":\"2020-05-04T18:18:03.686Z\",\"timestampType\":0},\"name\":\"sensor_45\",\"temperature\":134.04759,\"someLong\":1588616283685,\"someStuff\":\"even\",\"someNumber\":95}",
    "severity": "CRITICAL",
    "source": "streamingSource1",
    "sourceId": "sensor_45",
    "timestamp": "2020-05-04T18:18:04.651Z"
  },
  {
    "eventId": "01c0e3e8-919e-4171-b528-cb606e41896b",
    "eventRuleName": "HighTemperature",
    "eventType": "TempControl",
    "payload": "{\"kafkaMetadata\":{\"key\":\"NDUyNzgyNjQx\",\"headers\":[],\"topic\":\"fake-data.topic\",\"partition\":1,\"offset\":2169,\"timestamp\":\"2020-05-04T18:18:56.639Z\",\"timestampType\":0},\"name\":\"sensor_45\",\"temperature\":107.495094,\"someLong\":1588616336638,\"someStuff\":\"even\",\"someNumber\":92}",
    "severity": "CRITICAL",
    "source": "streamingSource1",
    "sourceId": "sensor_45",
    "timestamp": "2020-05-04T18:18:57.450Z"
  }]"""

  override def events(search: String,
                      startTimestamp: Instant,
                      endTimestamp: Instant,
                      page: Int,
                      size: Int): Future[Events] = {

    val result = data
      .filter(x => x.payload.contains(search))
      .filter(x => x.timestamp.isAfter(startTimestamp))
      .filter(x => x.timestamp.isBefore(endTimestamp))

    Future.successful(
      Events(result.length, result.slice(page * size, page * size + size))
    )

  }
}

class EventsControllerSpec
    extends FlatSpec
    with ScalatestRouteTest
    with Matchers
    with JsonSupport {
  implicit def angularResponse[T: JsonFormat]
    : RootJsonFormat[AngularResponse[T]] = jsonFormat2(AngularResponse.apply[T])

  implicit val timeout: RouteTestTimeout = RouteTestTimeout(10.seconds.dilated)

  it should "Respond to get requests" in {
    val service: EventsService = new MockEventsService
    val controller = new EventController(service)

    Get(
      s"/events?search=headers&startTimestamp=2020-05-04T18:18:57.000Z&endTimestamp=${Instant.now().toString}&page=0&size=100"
    ) ~> controller.events(false) ~> check {
      val response = responseAs[AngularResponse[Events]]

      response.data.found should be(1)
      response.data.entries.length should be(1)
      response.data.entries.head.eventId should be ("01c0e3e8-919e-4171-b528-cb606e41896b")
    }

  }

}
