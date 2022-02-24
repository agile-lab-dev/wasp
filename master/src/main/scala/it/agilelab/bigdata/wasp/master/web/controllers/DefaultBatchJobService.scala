package it.agilelab.bigdata.wasp.master.web.controllers

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.masterGuardian
import it.agilelab.bigdata.wasp.core.messages.StartBatchJob
import it.agilelab.bigdata.wasp.models.{BatchJobInstanceModel, BatchJobModel}
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL

object DefaultBatchJobService extends BatchJobService {
  override def delete(batchJob: BatchJobModel): Unit = {
    ConfigBL.batchJobBL.deleteByName(batchJob.name)
  }

  override def get(name: String): Option[BatchJobModel] = {
    ConfigBL.batchJobBL.getByName(name)
  }

  override def list(): Seq[BatchJobModel] = {
    ConfigBL.batchJobBL.getAll
  }

  override def insert(batchJob: BatchJobModel): Unit = {
    ConfigBL.batchJobBL.insert(batchJob)
  }

  override def update(batchJob: BatchJobModel): Unit = {
    ConfigBL.batchJobBL.update(batchJob)
  }

  override def start(name: String,
                     restConfig: Config): Either[String, String] = {
    WaspSystem.??[Either[String, String]](
      masterGuardian,
      StartBatchJob(name, restConfig)
    )
  }
  override def instance(instanceName: String): Option[BatchJobInstanceModel] = {
    ConfigBL.batchJobBL
      .instances()
      .getByName(instanceName)
  }

  override  def instanceOf(name: String): Seq[BatchJobInstanceModel] = {
    ConfigBL.batchJobBL
      .instances()
      .instancesOf(name)
      .sortBy { instance =>
        -instance.startTimestamp
      }
  }
}
