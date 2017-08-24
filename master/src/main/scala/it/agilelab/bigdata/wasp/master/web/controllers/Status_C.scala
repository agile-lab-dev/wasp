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


  val helpApi = Map(
    "wasp" -> Map(
      "/pipegraphs" ->  Map(
        "GET" -> "Get all the pipegraph in the system.",
        "POST" -> "Insert a new pipegraph.",
        "PUT" -> "Update an existing pipegraph."
      ),
      "/pipegraphs/{id}" ->  Map(
        "GET" -> "Get the pipegraph with the specified id.",
        "DELETE" -> "\tDelete the pipegraph with the specified id."
        ),
      "/pipegraphs/name/{name}" ->  Map(
        "GET" -> "Get the pipegraph with the specified name."
      ),
      "/pipegraphs/{id}/start" ->  Map(
        "POST" -> "Start the pipegraph with the specified id."
      ),
      "/pipegraphs/{id}/stop" ->  Map(
        "POST" -> "Stop the pipegraph with the specified id."
      ),
      "/producers" ->  Map(
        "GET" -> "Get all the procuders in the system.",
        "PUT" -> "Update an existing pipegraph."
      ),
      "/producers/{id}" ->  Map(
        "GET" -> "Get the producer with the specified id."
      ),
      "/producers/{id}/start" ->  Map(
        "POST" -> "Start the producer with the specified id."
      ),
      "/producers/{id}/stop" ->  Map(
        "POST" -> "Stop the producer with the specified id."
      ),
      "/topics" ->  Map(
        "GET" -> "Get all the topics in the system."
      ),
      "/topics/{id}" ->  Map(
        "GET" -> "Get the producer with the specified id."
      ),
      "/batchjobs" ->  Map(
        "GET" -> "Get all the batchjobs in the system.",
        "POST" -> "Insert a new batchjobs.",
        "PUT" -> "Update an existing batchjobs."
      ),
      "/batchjobs/{id}" ->  Map(
        "GET" -> "Get the batchjobs with the specified id.",
        "DELETE" -> "Delete the batchjobs with the specified id."
      ),
      "/batchjobs/{id}/start" ->  Map(
        "POST" -> "Start the batchjobs with the specified id."
      ),
      "/index/{name}" ->  Map(
        "GET" -> "Get the pipegraph with the specified name."
      ),
      "/mlmodels" ->  Map(
        "GET" -> "Get all the ML models in the system.",
        "PUT" -> "Update an existing ML models."
      ),
      "/mlmodels/{id}" ->  Map(
        "GET" -> "Get the ML models with the specified id.",
        "DELETE" -> "Delete the ML models with the specified id."
      ),
      "/configs/kafka" ->  Map(
        "GET" -> "Get the Kakfa configuration."
      ),
      "/configs/sparkbatch" ->  Map(
        "GET" -> "Get the Spark batch configuration."
      ),
      "/configs/sparkstreaming" ->  Map(
        "GET" -> "Get the Spark streaming configuration."
      ),
      "/configs/es" ->  Map(
        "GET" -> "Get the Elasticsearch configuration. If exists."
      ),
      "/configs/solr" ->  Map(
        "GET" -> "Get the Solr configuration. If exists."
      ),
    )
  ).toJson


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
    } ~
      pathPrefix("help") {
        pathEnd {
          get {
            complete {
              httpResponseJson(entity = helpApi.prettyPrint)
            }
          }
      } ~
        pathEnd {
          get {
            complete {
              httpResponseJson(entity = helpApi.prettyPrint)
            }
          }
        }

}
