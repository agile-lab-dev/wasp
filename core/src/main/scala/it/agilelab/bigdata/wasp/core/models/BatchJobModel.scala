package it.agilelab.bigdata.wasp.core.models

import org.mongodb.scala.bson.BsonObjectId

object JobStateEnum extends Enumeration {
  type JobState = Value
  val PENDING = "PENDING"
  val PROCESSING = "PROCESSING"
  val SUCCESSFUL = "SUCCESSFUL"
  val FAILED = "FAILED"
}

case class BatchJobModel(override val name: String,
                         description: String,
                         owner: String,
                         system: Boolean,
                         creationTime: Long,
                         etl: BatchETLModel,
                         var state: String,
                         _id: Option[BsonObjectId] = None)
  extends Model

case class BatchETLModel(name: String,
                         inputs: List[ReaderModel],
                         output: WriterModel,
                         mlModels: List[MlModelOnlyInfo],
                         strategy: Option[StrategyModel],
                         kafkaAccessType: String,
                         group: String = "default",
                         var isActive: Boolean = false)