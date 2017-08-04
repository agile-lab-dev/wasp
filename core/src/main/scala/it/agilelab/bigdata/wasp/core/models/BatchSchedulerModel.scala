package it.agilelab.bigdata.wasp.core.models

import org.mongodb.scala.bson.{BsonDocument, BsonObjectId}

case class BatchSchedulerModel (override val name: String,
                                cronExpression: String,
                                batchJob: Option[BsonObjectId],
                                options: Option[BsonDocument] = None,
                                isActive: Boolean = true,
                                _id: Option[BsonObjectId] = None
                                 ) extends Model