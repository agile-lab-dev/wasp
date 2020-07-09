package it.agilelab.bigdata.wasp.master.web.controllers

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.models.{BatchJobInstanceModel, BatchJobModel}

trait BatchJobService {

  def list(): Seq[BatchJobModel]

  def insert(batchJob: BatchJobModel): Unit

  def update(batchJob: BatchJobModel): Unit

  def start(name: String, restConfig: Config): Either[String, String]

  def instance(instanceName: String): Option[BatchJobInstanceModel]

  def instanceOf(name: String): Seq[BatchJobInstanceModel]

  def delete(batchJob: BatchJobModel): Unit

  def get(name: String): Option[BatchJobModel]
}
