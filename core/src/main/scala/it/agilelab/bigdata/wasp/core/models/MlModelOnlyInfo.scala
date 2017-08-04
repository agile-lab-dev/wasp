package it.agilelab.bigdata.wasp.core.models

import org.joda.time.DateTime
import org.mongodb.scala.bson.BsonObjectId

case class MlModelOnlyInfo(name: String, version: String, className: Option[String] = None,
                           timestamp: Option[Long] = None, modelFileId: Option[BsonObjectId] = None,
                           favorite: Boolean = false, description: String = "", _id: Option[BsonObjectId] = None
                            ) extends Model