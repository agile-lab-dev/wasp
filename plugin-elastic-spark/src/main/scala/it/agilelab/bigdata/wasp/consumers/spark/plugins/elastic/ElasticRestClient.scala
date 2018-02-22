package it.agilelab.bigdata.wasp.consumers.spark.plugins.elastic

trait ElasticRestClient extends AutoCloseable {
  def addAlias(index:String, alias: String) : Boolean
  def addIndex(index:String, settings: Option[String] = None): Boolean
  def addMapping(index:String, dataType:String, mapping:String): Boolean
  def checkIndex(index:String):Boolean
  def removeAlias(index: String, alias: String):Boolean
  def removeIndex(index: String): Boolean
}
