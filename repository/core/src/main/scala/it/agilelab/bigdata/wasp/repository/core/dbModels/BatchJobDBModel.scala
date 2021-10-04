package it.agilelab.bigdata.wasp.repository.core.dbModels

import com.typesafe.config.{Config, ConfigFactory}
import it.agilelab.bigdata.wasp.models.JobStatus.JobStatus
import it.agilelab.bigdata.wasp.models.{BatchETL, BatchJobExclusionConfig, Model}

trait BatchJobDBModel extends Model
trait BatchJobInstanceDBModel extends Model


case class BatchJobDBModelV1(override val name: String,
                           description: String,
                           owner: String,
                           system: Boolean,
                           creationTime: Long,
                           etl: BatchETL,
                           exclusivityConfig: BatchJobExclusionConfig = BatchJobExclusionConfig(
                             isFullyExclusive = true,
                             Seq.empty[String]
                           )) extends BatchJobDBModel

case class BatchJobInstanceDBModelV1(override val name: String,
                                     instanceOf: String,
                                     startTimestamp: Long,
                                     currentStatusTimestamp: Long,
                                     status: JobStatus,
                                     restConfig: Config = ConfigFactory.empty,
                                     error: Option[String] = None
                                    ) extends BatchJobInstanceDBModel