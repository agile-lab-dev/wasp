package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.{MongoDbProduct}
import it.agilelab.bigdata.wasp.datastores.DatastoreProduct

case class DocumentModel(override val name: String, connectionString: String, schema: String) extends DatastoreModel {
  override def datastoreProduct: DatastoreProduct = MongoDbProduct
}
