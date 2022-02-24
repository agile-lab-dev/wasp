package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import akka.http.scaladsl.unmarshalling.Unmarshaller
import it.agilelab.bigdata.wasp.models.Aggregate.Aggregate
import it.agilelab.bigdata.wasp.models.{Aggregate, MetricEntry, SourceEntry}
import it.agilelab.bigdata.wasp.utils.JsonSupport

import java.time.Instant

class TelemetryController(telemetry: TelemetryService) extends Directives with JsonSupport {

  val parseInstant: Unmarshaller[String, Instant] =
    Unmarshaller.identityUnmarshaller[String].map(x => Instant.parse(x))
  val parseAggregate: Unmarshaller[String, Aggregate] =
    Unmarshaller.identityUnmarshaller[String].map(name => Aggregate.withName(name))

  def getRoutes: Route = pretty(events(_))

  def pretty(subroute: Boolean => Route): Route =
    parameters('pretty.as[Boolean].?(false)) { pretty =>
      subroute(pretty)
    }

  def events(pretty: Boolean): Route = get {
    path("telemetry" / "sources") {
      parameter('search.as[String]) { search =>
        parameter('size.as[Int]) { size =>
          extractExecutionContext { implicit ec =>
            complete {
              import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.AngularOkResponse
              import spray.json._
              telemetry
                .sources(search, size)
                .map { x =>
                  x.toJson
                    .toAngularOkResponse(pretty)
                }
            }
          }
        }
      }
    } ~ path("telemetry" / "metrics") {
      parameter('search.as[String]) { search =>
        parameter('source.as[String]) { source =>
          parameter('size.as[Int]) { size =>
            extractExecutionContext { implicit ec =>
              complete {
                import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.AngularOkResponse
                import spray.json._
                telemetry
                  .metrics(SourceEntry(source), search, size)
                  .map { x =>
                    x.toJson
                      .toAngularOkResponse(pretty)
                  }
              }
            }
          }
        }
      }
    } ~ path("telemetry" / "series") {
      parameter('source.as[String]) { source =>
        parameter('metric.as[String]) { metric =>
          parameter('size.as[Int]) { size =>
            parameter('startTimestamp.as[Instant](parseInstant)) { startTimestamp =>
              parameter('endTimestamp.as[Instant](parseInstant)) { endTimestamp =>
                parameter('aggregate.as[Aggregate](parseAggregate)) { aggregate =>
                  extractExecutionContext { implicit ec =>
                    complete {
                      import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.AngularOkResponse
                      import spray.json._
                      telemetry
                        .values(
                          SourceEntry(source),
                          MetricEntry(SourceEntry(source), metric),
                          startTimestamp,
                          endTimestamp,
                          aggregate,
                          size
                        )
                        .map { x =>
                          x.toJson
                            .toAngularOkResponse(pretty)
                        }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
