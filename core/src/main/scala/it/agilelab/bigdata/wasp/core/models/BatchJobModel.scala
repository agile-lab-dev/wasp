package it.agilelab.bigdata.wasp.core.models

import com.typesafe.config.{Config, ConfigFactory}
import it.agilelab.bigdata.wasp.core.models.JobStatus.JobStatus


object JobStatus extends Enumeration {
  type JobStatus = Value

  val PENDING, PROCESSING, SUCCESSFUL, FAILED, STOPPED = Value
}


case class BatchJobModel(override val name: String,
                         description: String,
                         owner: String,
                         system: Boolean,
                         creationTime: Long,
                         etl: BatchETLModel)
	  extends Model



case class BatchJobInstanceModel(override val name:String,
                                 instanceOf: String,
                                 startTimestamp: Long,
                                 currentStatusTimestamp: Long,
                                 status: JobStatus,
                                 restConfig: Config = ConfigFactory.empty,
                                 error: Option[String] = None
                                ) extends Model

case class BatchETLModel(name: String,
                         inputs: List[ReaderModel],
                         output: WriterModel,
                         mlModels: List[MlModelOnlyInfo],
                         strategy: Option[StrategyModel],
                         kafkaAccessType: String,
                         group: String = "default",
                         var isActive: Boolean = false)