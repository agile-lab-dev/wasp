package it.agilelab.bigdata.wasp.consumers.spark.plugins.elastic

import org.elasticsearch.hadoop.cfg.{ConfigurationOptions, PropertiesSettings}
import org.elasticsearch.hadoop.rest.Request.Method
import org.elasticsearch.hadoop.rest.{Response, RestClient => HadoopEsRestClient}
import org.elasticsearch.hadoop.util.BytesArray

private[elastic] class InternalRestClient(settingsFactory: () => PropertiesSettings) extends HadoopEsRestClient(settingsFactory()) {

  def this(nodes: Seq[String]) = this(() => {
    val settings = new PropertiesSettings()
    settings.setProperty(ConfigurationOptions.ES_NODES, nodes.mkString(","))
    settings
  })


  def put(uri: String, body: String = ""): Response = {
    super.execute(Method.PUT, uri, new BytesArray(body.getBytes()), true)
  }

  def delete(uri: String, body: String = ""): Response = {
    super.execute(Method.DELETE, uri, new BytesArray(body.getBytes()), true)
  }

  def get(uri: String, body: String = ""): Response = {
    super.execute(Method.GET, uri, new BytesArray(body.getBytes()), true)
  }

  def post(uri: String, body: String = ""): Response = {
    super.execute(Method.POST, uri, new BytesArray(body.getBytes()), true)
  }

}


class HighLevelElasticRestClient(nodes: Seq[String]) extends ElasticRestClient {

  val internalClient = new InternalRestClient(nodes)

  def addAlias(index: String, alias: String) =
    internalClient.put(s"${index}/_alias/${alias}").hasSucceeded

  override def addIndex(index: String, settings: Option[String] = None): Boolean =
    internalClient.put(index, settings.getOrElse("")).hasSucceeded


  override def addMapping(index: String, dataType: String, mapping: String): Boolean =
    internalClient.put(s"${index}/_mapping/${dataType}", mapping).hasSucceeded


  override def checkIndex(index: String): Boolean =
    internalClient.indexExists(index)


  override def removeAlias(index: String, alias: String): Boolean =
    internalClient.delete(s"${index}/_alias/${alias}")

  override def removeIndex(index: String): Boolean =
    internalClient.delete(s"${index}")

  override def close(): Unit = internalClient.close()
}

