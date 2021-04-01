package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.WebMailProduct
import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct}

case class WebMailModel (override val name: String)
  extends DatastoreModel {
  override val datastoreProduct: DatastoreProduct = WebMailProduct
}