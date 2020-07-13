package it.agilelab.bigdata.wasp.master.web.controllers

import akka.cluster.Cluster
import akka.http.scaladsl.server.{Directives, Route}
import com.typesafe.config.ConfigRenderOptions
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper._
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

object Status_C extends Directives with JsonSupport {

  /** Return the available APIs as a JSON arrayOfObjects */
  private lazy val helpContentArray = Array(
    Map("/pipegraphs" -> Map(
      "GET" -> "Get all the pipegraph in the system",
      "POST" -> "Insert a new pipegraph",
      "PUT" -> "Update an existing pipegraph"
    )),
    Map("/pipegraphs/{id}" -> Map(
      "GET" -> "Get the pipegraph with the specified id",
      "DELETE" -> "Delete the pipegraph with the specified id"
    )),
    Map("/pipegraphs/{id}/start" -> Map(
      "POST" -> "Start the pipegraph with the specified id and return the related instance id"
    )),
    Map("/pipegraphs/{id}/stop" -> Map(
      "POST" -> "Stop the active (i.e. \"PROCESSING\") instance of the specified pipegraph"
    )),
    Map("/pipegraphs/{id}/instances" -> Map(
      "GET" -> "Get instances of the specified pipegraph, ordered newest to latest"
    )),
    Map("/pipegraphs/{pipegraphId}/instances/{instanceId}" -> Map(
      "GET" -> "Get instance status of the specified pipegraph instance"
    )),

    Map("/producers" -> Map(
      "GET" -> "Get all the procuders in the system",
      "PUT" -> "Update an existing producer"
    )),
    Map("/producers/{id}" -> Map(
      "GET" -> "Get the producer with the specified id"
    )),
    Map("/producers/{id}/start" -> Map(
      "POST" -> "Start the producer with the specified id"
    )),
    Map("/producers/{id}/stop" -> Map(
      "POST" -> "Stop the producer with the specified id"
    )),

    Map("/topics" -> Map(
      "GET" -> "Get all the topics in the system"
    )),
    Map("/topics/{id}" -> Map(
      "GET" -> "Get the producer with the specified id."
    )),

    Map("/batchjobs" -> Map(
      "GET" -> "Get all the batchjobs in the system",
      "POST" -> "Insert a new batchjob",
      "PUT" -> "Update an existing batchjob"
    )),
    Map("/batchjobs/{id}" -> Map(
      "GET" -> "Get the batchjobs with the specified id",
      "DELETE" -> "Delete the batchjob with the specified id"
    )),
    Map("/batchjobs/{id}/start" -> Map(
      "POST" -> "Start an instance (with optional JSON configuration) of the specified batchjob and return the related instance id"
    )),
    Map("/batchjobs/{id}/instances" -> Map(
      "GET" -> "Get instances of the specified batchjob, ordered newest to latest"
    )),
    Map("/batchjobs/{batchjobId}/instances/{instanceId}" -> Map(
      "GET" -> "Get instance status of the specified batchjob instance"
    )),

    Map("/indexes" -> Map(
      "GET" -> "Get all the indexes in the system"
    )),
    Map("/indexes/{id}" -> Map(
      "GET" -> "Get the index with the specified id"
    )),

    Map("/mlmodels" -> Map(
      "GET" -> "Get all the ML models in the system",
      "PUT" -> "Update an existing ML models"
    )),
    Map("/mlmodels/{id}" -> Map(
      "GET" -> "Get the ML models with the specified id",
      "DELETE" -> "Delete the ML models with the specified id"
    )),

    Map("/configs/kafka" -> Map(
      "GET" -> "Get the Kakfa configuration"
    )),
    Map("/configs/sparkbatch" -> Map(
      "GET" -> "Get the Spark batch configuration"
    )),
    Map("/configs/sparkstreaming" -> Map(
      "GET" -> "Get the Spark streaming configuration"
    )),
    Map("/configs/es" -> Map(
      "GET" -> "Get the Elasticsearch configuration"
    )),
    Map("/configs/solr" -> Map(
      "GET" -> "Get the Solr configuration"
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
            val mongoDBConfigurations = ConfigBL.dBConfigBL.retrieveDBConfig().map(JsonParser(_)).toVector.toJson

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