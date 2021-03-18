package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.models.CdcModel
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object CdcController extends Directives with JsonSupport {

  def getRoute: Route = {
     pathPrefix("cdc") {
        parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
          pathEnd {
            get {
              complete {
                // complete with serialized Future result
                getJsonArrayOrEmpty[CdcModel](ConfigBL.cdcBL.getAll(), _.toJson, pretty)
              }
            }
          } ~
            path(Segment) { name =>
              get {
                complete {
                  // complete with serialized Future result
                  getJsonOrNotFound[CdcModel](ConfigBL.cdcBL.getByName(name), name, "Cdc model", _.toJson, pretty)
                }
              }
            }
        }
      }
  }
}