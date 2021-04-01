package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.genericProduct
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct
import it.agilelab.bigdata.wasp.models.DatastoreModel

case class ParallelWriteModel(
															mode: String,
															format: String,
															partitionBy: Option[List[String]],
															requestBody: ParallelWriteBody
														 )  extends DatastoreModel {

	override def datastoreProduct: DatastoreProduct = genericProduct

	override val name: String = "name"
}


case class ParallelWriteBody(source: String)