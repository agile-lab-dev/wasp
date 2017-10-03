package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.WaspSystem
import org.mongodb.scala.bson.BsonObjectId

object JobStateEnum extends Enumeration {
  type JobState = Value
  val PENDING = "PENDING"
  val PROCESSING = "PROCESSING"
  val SUCCESSFUL = "SUCCESSFUL"
  val FAILED = "FAILED"

}

case class BatchJobModel(
                          override val name: String,
                          description: String,
                          owner: String,
                          system: Boolean,
                          creationTime: Long,
                          etl: StreamingModel,
                          var state: String,
                          _id: Option[BsonObjectId] = None
                          ) extends Model {

}