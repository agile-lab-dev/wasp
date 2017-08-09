package it.agilelab.bigdata.wasp.web.controllers


import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.web.utils.JsonSupport
import spray.json._

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */


object Topic_C extends Directives with JsonSupport {

  val logger = WaspLogger(Topic_C.getClass.getName)

  def getRoute: Route = {
    // extract URI path element as Int
    pathPrefix("topics") {
      pathEnd {
        get {
          complete {
            // complete with serialized Future result
            ConfigBL.topicBL.getAll.toJson
          }
        }
      } ~
        path(Segment) { id =>
          get {
            complete {
              // complete with serialized Future result
              ConfigBL.topicBL.getById(id).toJson
            }

          }

        }
    }
  }
}
