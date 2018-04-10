package it.agilelab.bigdata.wasp.master.web.controllers

import akka.cluster.Cluster
import akka.http.scaladsl.server.{Directives, Route}
import com.typesafe.config.ConfigRenderOptions
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.utils.MongoDBHelper._
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, WaspDB}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import spray.json._

object Status_C extends Directives with JsonSupport {

  /** Return the available APIs as a JSON arrayOfObjects */
  private lazy val helpContentArray = Array(
    Map("/pipegraphs" -> Map(
      "GET" -> "Get all the pipegraph in the system.",
      "POST" -> "Insert a new pipegraph.",
      "PUT" -> "Update an existing pipegraph."
    )),
    Map("/pipegraphs/{name}" -> Map(
      "GET" -> "Get the pipegraph with the specified name.",
      "DELETE" -> "Delete the pipegraph with the specified name."
    )),
    Map("/pipegraphs/{name}/start" -> Map(
      "POST" -> "Start the pipegraph with the specified name."
    )),
    Map("/pipegraphs/{name}/stop" -> Map(
      "POST" -> "Stop the pipegraph with the specified name."
    )),
    Map("/pipegraphs/{name}/instances" -> Map(
      "GET" -> "Get instances of pipegraph with the specified name, ordered newest to oldest"
    )),
    Map("/pipegraphs/{pipegraphName}/instances/{instanceName}" -> Map(
      "GET" -> "Get instance status of pipegraph instance with the specified name"
    )),
    Map("/producers" -> Map(
      "GET" -> "Get all the procuders in the system.",
      "PUT" -> "Update an existing pipegraph."
    )),
    Map("/producers/{name}" -> Map(
      "GET" -> "Get the producer with the specified name."
    )),
    Map("/producers/{name}/start" -> Map(
      "POST" -> "Start the producer with the specified name."
    )),
    Map("/producers/{name}/stop" -> Map(
      "POST" -> "Stop the producer with the specified name."
    )),
    Map("/topics" -> Map(
      "GET" -> "Get all the topics in the system."
    )),
    Map("/topics/{name}" -> Map(
      "GET" -> "Get the producer with the specified name."
    )),
    Map("/batchjobs" -> Map(
      "GET" -> "Get all the batchjobs in the system.",
      "POST" -> "Insert a new batchjobs.",
      "PUT" -> "Update an existing batchjobs."
    )),
    Map("/batchjobs/{name}" -> Map(
      "GET" -> "Get the batchjobs with the specified id.",
      "DELETE" -> "Delete the batchjobs with the specified name."
    )),
    Map("/batchjobs/{name}/start" -> Map(
      "POST" -> "Start the batchjobs with the specified name."
    )),
    Map("/batchjobs/{name}/instances" -> Map(
      "GET" -> "Get instances of batchjob with the specified name, ordered newest to oldest"
    )),
    Map("/batchjobs/{batchjobName}/instances/{instanceName}" -> Map(
      "GET" -> "Get instance status of batchjob instance with the specified name"
    )),
    Map("/index/{name}" -> Map(
      "GET" -> "Get the index with the specified name."
    )),
    Map("/indexes" -> Map(
      "GET" -> "Get all the indexes."
    )),
    Map("/indexes/{name}" -> Map(
      "GET" -> "Get the index with the specified name."
    )),
    Map("/mlmodels" -> Map(
      "GET" -> "Get all the ML models in the system.",
      "PUT" -> "Update an existing ML models."
    )),
    Map("/mlmodels/{name}/{version}" -> Map(
      "GET" -> "Get the ML models with the specified name and version.",
      "DELETE" -> "Delete the ML models with the specified name and version."
    )),
    Map("/configs/kafka" -> Map(
      "GET" -> "Get the Kakfa configuration."
    )),
    Map("/configs/sparkbatch" -> Map(
      "GET" -> "Get the Spark batch configuration."
    )),
    Map("/configs/sparkstreaming" -> Map(
      "GET" -> "Get the Spark streaming configuration."
    )),
    Map("/configs/es" -> Map(
      "GET" -> "Get the Elasticsearch configuration. If exists."
    )),
    Map("/configs/solr" -> Map(
      "GET" -> "Get the Solr configuration. If exists."
    ))
  )

  val helpApi = Map(
    "wasp" -> helpContentArray
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
                "actorSystemName" -> WaspSystem.actorSystem.name.toJson,
                "clusterMembers" -> members,
                "mongoDBConfigurations" -> mongoDBConfigurations,
                "configurations" -> waspConfig
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
        }
      } ~
      pathPrefix("") {
        pathEnd {
          get {
            complete {
              httpResponseJson(entity = helpApi.prettyPrint)
            }
          }
        }
      }
  }
}