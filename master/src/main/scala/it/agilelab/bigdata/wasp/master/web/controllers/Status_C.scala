package it.agilelab.bigdata.wasp.master.web.controllers


import akka.cluster.Cluster
import akka.http.scaladsl.server.{Directives, Route}
import com.typesafe.config.ConfigRenderOptions
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, WaspDB}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import spray.json._
import it.agilelab.bigdata.wasp.core.utils.MongoDBHelper._



object Status_C extends Directives with JsonSupport {
  def getRoute: Route = {

    pathPrefix("status") {
      pathEnd {
        get {
          complete {
            val cluster = Cluster(WaspSystem.actorSystem)
            val members = cluster.state.members.map(m => Map(
              "node" -> m.address.toString.toJson,
              "status" -> m.status.toString.toJson,
              "roles" -> m.roles.toVector.toJson
            ).toJson).toVector.toJson

            val waspConfig = JsonParser(ConfigManager.conf.root().render(ConfigRenderOptions.concise()))
            val mongoDBConfigurations =
              WaspDB.getDB.mongoDatabase.getCollection(WaspDB.configurationsName)
                .find().results().map(_.toJson()).map(JsonParser(_)).toVector.toJson

            val result = Map(
              "wasp" -> Map(
                "actorSystemName" ->  WaspSystem.actorSystem.name.toJson,
                "clusterMembers" -> members,
                "mongoDBConfigurations" -> mongoDBConfigurations,
                "configurations"   -> waspConfig
              )
            ).toJson
            httpResponseJson(entity = result.prettyPrint)
          }
        }
      }
    }
  }

}
