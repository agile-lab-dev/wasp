package it.agilelab.bigdata.wasp.consumers.spark.plugins.continuous.model

import it.agilelab.bigdata.microservicecatalog.entity.WriteExecutionPlanRequestBody
import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct, GenericProduct}
import it.agilelab.bigdata.wasp.models.DatastoreModel

case class ContinuousUpdateModel(requestBody: WriteExecutionPlanRequestBody, keys: List[String], tableName: String, orderingExpression: String, fieldsToDrop: List[String] = List.empty, entityDetails: Map[String, String], s3aEndpoint: String) extends DatastoreModel {


  override def datastoreProduct: DatastoreProduct = GenericProduct("continuous", Some("continuousUpdate"))

  override val name: String = "name"
}

