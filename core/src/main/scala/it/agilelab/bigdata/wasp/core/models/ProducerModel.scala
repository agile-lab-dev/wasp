package it.agilelab.bigdata.wasp.core.models

import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}


/** DataSource class. The fields must be the same as the ones inside the MongoDB document associated with this model **/
case class ProducerModel(override val name: String,
                         className: String,
                         topicName: Option[String],
                         var isActive: Boolean = false,
                         configuration: Option[String] = None,
                         isRemote: Boolean,
                         isSystem: Boolean) extends Model {

  def hasOutput: Boolean = topicName.isDefined

}