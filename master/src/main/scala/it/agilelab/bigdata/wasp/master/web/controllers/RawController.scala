package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.models.{KeyValueModel, RawModel}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import spray.json._

object RawController extends Directives with JsonSupport {

  def getRoute: Route = {
     pathPrefix("raw") {
        parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
          pathEnd {
            get {
              complete {
                // complete with serialized Future result
                getJsonArrayOrEmpty[RawModel](ConfigBL.rawBL.getAll(), _.toJson, pretty)
              }
            }
          } ~
            path(Segment) { name =>
              get {
                complete {
                  // complete with serialized Future result
                  getJsonOrNotFound[RawModel](ConfigBL.rawBL.getByName(name), name, "Keyvalue model", _.toJson, pretty)
                }
              }
            }
        }
      }
  }
}