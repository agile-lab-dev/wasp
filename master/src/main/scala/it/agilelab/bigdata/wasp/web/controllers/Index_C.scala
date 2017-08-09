package it.agilelab.bigdata.wasp.web.controllers

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.web.utils.JsonSupport
import spray.json.{DefaultJsonProtocol, _}

/**
 * Created by vitoressa on 12/10/15.
 */


object Index_C extends Directives with JsonSupport {

  val logger = WaspLogger(Index_C.getClass.getName)

  def getRoute: Route = {
    // getByName
    pathPrefix("index" / Segment) { name =>
      pathEnd {
        get {
          complete {
            // complete with serialized Future result
            ConfigBL.indexBL.getByName(name).get.toJson
          }

        }
      }

    }
  }
}
