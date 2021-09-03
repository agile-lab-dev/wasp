package it.agilelab.bigdata.wasp.models

import org.mongodb.scala.bson.BsonObjectId

case class MlModelOnlyInfo(
    name: String,
    version: String,
    className: Option[String] = None,
    timestamp: Option[Long] = None,
    modelFileId: Option[BsonObjectId] = None,
    favorite: Boolean = false,
    description: String = ""
) extends Model
