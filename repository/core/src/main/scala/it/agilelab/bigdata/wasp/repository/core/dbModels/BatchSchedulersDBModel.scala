package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.Model
import org.mongodb.scala.bson.BsonDocument

trait BatchSchedulerDBModel extends Model

case class BatchSchedulerDBModelV1(override val name: String,
                                    cronExpression: String,
                                    batchJob: Option[String],
                                    options: Option[BsonDocument] = None,
                                    isActive: Boolean = true
                                   ) extends BatchSchedulerDBModel