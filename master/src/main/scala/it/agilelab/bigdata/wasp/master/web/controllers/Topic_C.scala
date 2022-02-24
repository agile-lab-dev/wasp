package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.models.DatastoreModel
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
object Topic_C extends Directives with JsonSupport {

  def getRoute: Route = {
    pathPrefix("topics") {
      parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
        pathEnd {
          get {
            complete {
              // complete with serialized Future result
              getJsonArrayOrEmpty[DatastoreModel](ConfigBL.topicBL.getAll, _.toJson, pretty)
            }
          }
        } ~
          path(Segment) { name =>
            get {
              complete {
                // complete with serialized Future result
                getJsonOrNotFound[DatastoreModel](ConfigBL.topicBL.getByName(name), name, "Topic model", _.toJson, pretty)
              }
            }
          }
      }
    }
  }
}