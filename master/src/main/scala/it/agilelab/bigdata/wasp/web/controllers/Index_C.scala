package it.agilelab.bigdata.wasp.web.controllers

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.IndexModel
import spray.json.{DefaultJsonProtocol, _}

/**
 * Created by vitoressa on 12/10/15.
 */

// collect your json format instances into a support trait:
trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  import it.agilelab.bigdata.wasp.web.utils.BsonConvertToSprayJson._
  implicit val indexModelFormat: RootJsonFormat[IndexModel] = jsonFormat7(IndexModel.apply)
}

object Index_C extends Directives with JsonSupport {

  val logger = WaspLogger("Index")

  def getRoute: Route = {
    // extract URI path element as Int
    pathPrefix("index" / Segment) { name =>
      pathEnd {
        get {
          complete {
            // complete with serialized Future result
            ConfigBL.indexBL.getByName(name).get.schema.get.toJson
          }

        }
      }

    }
  }
}
