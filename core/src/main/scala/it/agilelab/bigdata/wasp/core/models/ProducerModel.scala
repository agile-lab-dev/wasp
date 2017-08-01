package it.agilelab.bigdata.wasp.core.models

import reactivemongo.bson.{BSONDocument, BSONObjectID}


/** DataSource class. The fields must be the same as the ones inside the MongoDB document associated with this model **/
case class ProducerModel(override val name: String,
                         className: String,
                         id_topic: Option[BSONObjectID],
                         var isActive: Boolean,
                         configuration: Option[BSONDocument] = None,
                         isRemote: Boolean,
                         _id: Option[BSONObjectID] = None) extends Model {

  def hasOutput: Boolean = id_topic.isDefined

}