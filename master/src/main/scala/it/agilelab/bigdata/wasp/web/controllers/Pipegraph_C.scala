package it.agilelab.bigdata.wasp.web.controllers

import java.util.concurrent.TimeUnit

import akka.http.scaladsl.server.{Directives, Route}
import akka.pattern.ask
import akka.util.Timeout
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.messages.{StartPipegraph, StopPipegraph}
import it.agilelab.bigdata.wasp.core.models.PipegraphModel
import it.agilelab.bigdata.wasp.web.utils.JsonSupport
import spray.json._

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */


object Pipegraph_C extends Directives with JsonSupport {

  val logger = WaspLogger(Pipegraph_C.getClass.getName)
  //TODO prendere il timeout dalla configurazione
  //implicit val timeout = Timeout(ConfigManager.config)
  implicit val timeout = Timeout(30, TimeUnit.SECONDS)

  def getRoute: Route = {
    // extract URI path element as Int
    pathPrefix("pipegraphs") {
      pathEnd {
        get {
          complete {
            // complete with serialized Future result
            ConfigBL.pipegraphBL.getAll.toJson
          }
        } ~
          post {
            // unmarshal with in-scope unmarshaller
            entity(as[PipegraphModel]) { pipegraph =>
              complete {
                // complete with serialized Future result
                ConfigBL.pipegraphBL.insert(pipegraph)
                "OK".toJson
              }
            }
          } ~
          put {
            // unmarshal with in-scope unmarshaller
            entity(as[PipegraphModel]) { pipegraph =>
              complete {
                // complete with serialized Future result
                ConfigBL.pipegraphBL.update(pipegraph)
                "OK".toJson
              }
            }
          }
      } ~
        pathPrefix(Segment) { id =>
            path("start") {
              post {
                complete {
                  // complete with serialized Future result
                  WaspSystem.masterActor ? StartPipegraph(id)
                  "OK".toJson
                }
              }
            } ~
            path("stop") {
              post {
                complete {
                  // complete with serialized Future result
                  WaspSystem.masterActor ? StopPipegraph(id)
                  "OK".toJson
                }
              }
            } ~
          pathEnd {
            get {
              complete {
                // complete with serialized Future result
                ConfigBL.pipegraphBL.getById(id).toJson
              }
            } ~
              delete {
                complete {
                  // complete with serialized Future result
                  ConfigBL.pipegraphBL.deleteById(id).toJson
                }

              }
          }
        } ~
        path("name" / Segment) { name: String =>
          get {
            complete {
              // complete with serialized Future result
              ConfigBL.pipegraphBL.getByName(name).toJson
            }

          }
        }
    }
  }
}