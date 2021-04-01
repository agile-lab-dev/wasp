package it.agilelab.bigdata.wasp.models

import it.agilelab.bigdata.wasp.datastores.DatastoreProduct.IndexProduct
import it.agilelab.bigdata.wasp.datastores.{DatastoreProduct}
import it.agilelab.bigdata.wasp.utils.ConfigManagerHelper


case class IndexModel(override val name: String,
                      creationTime: Long,
                      schema: Option[String],
                      query: Option[String] = None,
                      numShards: Option[Int] = Some(1),
                      replicationFactor: Option[Int] = Some(1),
                      rollingIndex: Boolean = true,
                      idField: Option[String] = None,
                      options: Map[String, String] = Map.empty)
    extends DatastoreModel {

  def resource = s"$eventuallyTimedName/$dataType"

  def collection = eventuallyTimedName

  def eventuallyTimedName = if (rollingIndex) ConfigManagerHelper.buildTimedName(name) else name
  
  /**
    * Returns a JSON representation of the schema of this index's schema.
    * @return
    */
  def getJsonSchema: String = {
    schema.getOrElse("")
  }

  def dataType: String = name

  override def datastoreProduct: DatastoreProduct = IndexProduct
}