package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model

import it.agilelab.bigdata.microservicecatalog.entity.WriteExecutionPlanRequestBody
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.genericProduct
import it.agilelab.bigdata.wasp.models.DatastoreModel

case class ParallelWriteModel(mode: String, format: String, partitionBy: Option[List[String]], requestBody: WriteExecutionPlanRequestBody, entityDetails: Map[String, String], s3aEndpoint: String) extends DatastoreModel {

	override def datastoreProduct: DatastoreProduct = genericProduct

	override val name: String = "name"
}

