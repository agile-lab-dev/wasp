package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import spray.json._

/**
 * Created by vitoressa on 12/10/15.
 */
object Index_C extends Directives with JsonSupport {
  def getRoute: Route = {
    // getByName
    pathPrefix("index" / Segment) { name =>
      pathEnd {
        get {
          complete {
            // complete with serialized Future result
            getJsonOrNotFound[IndexModel](ConfigBL.indexBL.getByName(name), name, "Index model", _.toJson)
          }

        }
      }
    } ~
      pathPrefix("indexes") {
        pathEnd {
          get {
            complete {
              // complete with serialized Future result
              getJsonArrayOrEmpty[IndexModel](ConfigBL.indexBL.getAll(), _.toJson)
            }
          }
        } ~
          path(Segment) { name =>
            get {
              complete {
                // complete with serialized Future result
                getJsonOrNotFound[IndexModel](ConfigBL.indexBL.getByName(name), name, "Index model", _.toJson)
              }

            }

          }
      }
  }
}
