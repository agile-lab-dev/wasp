package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.models.{DocumentModel, KeyValueModel}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import spray.json._

object KeyValueController extends Directives with JsonSupport {

  def getRoute: Route = {
     pathPrefix("keyvalue") {
        parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
          pathEnd {
            get {
              complete {
                // complete with serialized Future result
                getJsonArrayOrEmpty[KeyValueModel](ConfigBL.keyValueBL.getAll(), _.toJson, pretty)
              }
            }
          } ~
            path(Segment) { name =>
              get {
                complete {
                  // complete with serialized Future result
                  getJsonOrNotFound[KeyValueModel](ConfigBL.keyValueBL.getByName(name), name, "Keyvalue model", _.toJson, pretty)
                }
              }
            }
        }
      }
  }
}