package it.agilelab.bigdata.wasp.repository.core.bl

import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel}

trait PipegraphInstanceBl {

  def getByName(name: String): Option[PipegraphInstanceModel]

  def insert(instance: PipegraphInstanceModel): PipegraphInstanceModel

  def update(instance: PipegraphInstanceModel): PipegraphInstanceModel

  def all(): Seq[PipegraphInstanceModel]

  def instancesOf(name: String): Seq[PipegraphInstanceModel]
}



trait PipegraphBL {

  def getByName(name: String): Option[PipegraphModel]

  def getAll: Seq[PipegraphModel]

  def getSystemPipegraphs: Seq[PipegraphModel]

  def getNonSystemPipegraphs: Seq[PipegraphModel]

  def getActivePipegraphs(): Seq[PipegraphModel]

  def insert(pipegraph: PipegraphModel): Unit

  def upsert(pipegraph: PipegraphModel): Unit

  def insertIfNotExists(pipegraph: PipegraphModel): Unit

  def update(pipegraphModel: PipegraphModel): Unit

  def deleteByName(id_string: String): Unit

  def instances(): PipegraphInstanceBl

}


