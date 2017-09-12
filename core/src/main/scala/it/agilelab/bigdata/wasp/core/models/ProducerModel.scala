package it.agilelab.bigdata.wasp.core.models

import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}


/** DataSource class. The fields must be the same as the ones inside the MongoDB document associated with this model **/
case class ProducerModel(override val name: String,
                         className: String,
                         id_topic: Option[BsonObjectId],
                         var isActive: Boolean,
                         configuration: Option[BsonDocument] = None,
                         isRemote: Boolean,
                         isSystem: Boolean,
                         _id: Option[BsonObjectId] = None) extends Model {

  def hasOutput: Boolean = id_topic.isDefined

}