package it.agilelab.bigdata.wasp.core.models

import org.mongodb.scala.bson.BsonDocument

case class BatchSchedulerModel (override val name: String,
                                cronExpression: String,
                                batchJob: Option[String],
                                options: Option[BsonDocument] = None,
                                isActive: Boolean = true) extends Model