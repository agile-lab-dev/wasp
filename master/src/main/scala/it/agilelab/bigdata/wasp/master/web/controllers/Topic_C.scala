package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.datastores.TopicCategory
import it.agilelab.bigdata.wasp.core.models.{DatastoreModel, TopicModel}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
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
              getJsonArrayOrEmpty[DatastoreModel[TopicCategory]](ConfigBL.topicBL.getAll, _.toJson, pretty)
            }
          }
        } ~
          path(Segment) { name =>
            get {
              complete {
                // complete with serialized Future result
                getJsonOrNotFound[DatastoreModel[TopicCategory]](ConfigBL.topicBL.getByName(name), name, "Topic model", _.toJson, pretty)
              }
            }
          }
      }
    }
  }
}