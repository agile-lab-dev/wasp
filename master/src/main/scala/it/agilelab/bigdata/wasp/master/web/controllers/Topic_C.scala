package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.master.web.utils.{JsonResultsHelper, JsonSupport}
import spray.json._
import JsonResultsHelper._
import it.agilelab.bigdata.wasp.core.models.TopicModel


/**
  * Created by Agile Lab s.r.l. on 09/08/2017.
  */
object Topic_C extends Directives with JsonSupport {
  def getRoute: Route = {
    // extract URI path element as Int
    pathPrefix("topics") {
      pathEnd {
        get {
          complete {
            // complete with serialized Future result
            getJsonArrayOrEmpty[TopicModel](ConfigBL.topicBL.getAll, _.toJson)
          }
        }
      } ~
        path(Segment) { id =>
          get {
            complete {
              // complete with serialized Future result
              getJsonOrNotFound[TopicModel](ConfigBL.topicBL.getById(id), id, "Topic model", _.toJson)
            }

          }

        }
    }
  }
}
