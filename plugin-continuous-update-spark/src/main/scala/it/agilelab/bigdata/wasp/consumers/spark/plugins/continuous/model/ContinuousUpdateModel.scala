package it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.model

import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct, GenericProduct}
import it.agilelab.bigdata.wasp.models.DatastoreModel

case class ContinuousUpdateModel(requestBody: ParallelWriteBody, keys: List[String], tableName: String, orderingExpression: String, fieldsToDrop: List[String] = List.empty) extends DatastoreModel {


  override def datastoreProduct: DatastoreProduct = GenericProduct("continuous", Some("continuousUpdate"))

  override val name: String = "name"
}


case class ParallelWriteBody(source: String)