package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.{BatchJobInstanceModel, BatchJobModel}

trait BatchJobInstanceBL {

  def getByName(name: String): Option[BatchJobInstanceModel]

  def insert(instance: BatchJobInstanceModel): BatchJobInstanceModel

  def update(instance: BatchJobInstanceModel): BatchJobInstanceModel

  def all(): Seq[BatchJobInstanceModel]

  def instancesOf(name: String): Seq[BatchJobInstanceModel]
}

trait BatchJobBL {

  def getByName(name: String): Option[BatchJobModel]

  def getAll: Seq[BatchJobModel]

  def update(batchJobModel: BatchJobModel): Unit

  def insert(batchJobModel: BatchJobModel): Unit

  def upsert(batchJobModel: BatchJobModel): Unit

  def deleteByName(name: String): Unit

  def instances(): BatchJobInstanceBL

}





