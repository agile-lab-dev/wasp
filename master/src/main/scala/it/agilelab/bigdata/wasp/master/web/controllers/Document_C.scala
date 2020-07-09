package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.models.DocumentModel
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import spray.json._

/**
 * Created by vitoressa on 12/10/15.
 */
object Document_C extends Directives with JsonSupport {

  def getRoute: Route = {
    // Segment => indexName
    pathPrefix("document" / Segment) { name =>
      parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
        pathEnd {
          get {
            complete {
              // complete with serialized Future result
              getJsonOrNotFound[DocumentModel](ConfigBL.documentBL.getByName(name), name, "Document model", _.toJson, pretty)
            }

          }
        }
      }
    } ~
      pathPrefix("documents") {
        parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
          pathEnd {
            get {
              complete {
                // complete with serialized Future result
                getJsonArrayOrEmpty[DocumentModel](ConfigBL.documentBL.getAll(), _.toJson, pretty)
              }
            }
          } ~
            path(Segment) { name =>
              get {
                complete {
                  // complete with serialized Future result
                  getJsonOrNotFound[DocumentModel](ConfigBL.documentBL.getByName(name), name, "Document model", _.toJson, pretty)
                }
              }
            }
        }
      }
  }
}