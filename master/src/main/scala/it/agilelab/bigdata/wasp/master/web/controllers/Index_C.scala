package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import it.agilelab.bigdata.wasp.models.IndexModel
import spray.json._

/**
 * Created by vitoressa on 12/10/15.
 */
object Index_C extends Directives with JsonSupport {

  def getRoute: Route = {
    // Segment => indexName
    pathPrefix("index" / Segment) { name =>
      parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
        pathEnd {
          get {
            complete {
              // complete with serialized Future result
              getJsonOrNotFound[IndexModel](ConfigBL.indexBL.getByName(name), name, "Index model", _.toJson, pretty)
            }

          }
        }
      }
    } ~
      pathPrefix("indexes") {
        parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
          pathEnd {
            get {
              complete {
                // complete with serialized Future result
                getJsonArrayOrEmpty[IndexModel](ConfigBL.indexBL.getAll(), _.toJson, pretty)
              }
            }
          } ~
            path(Segment) { name =>
              get {
                complete {
                  // complete with serialized Future result
                  getJsonOrNotFound[IndexModel](ConfigBL.indexBL.getByName(name), name, "Index model", _.toJson, pretty)
                }
              }
            }
        }
      }
  }
}