package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{PipegraphModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.bson.{BsonBoolean, BsonString}
import org.mongodb.scala.bson.BsonObjectId

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait PipegraphBL  {

  def getByName(name: String): Option[PipegraphModel]

  def getById(id: String): Option[PipegraphModel]

  def getAll : Seq[PipegraphModel]

  def getSystemPipegraphs(isSystemPipegraph: Boolean = true): Seq[PipegraphModel]

  def getActivePipegraphs(isActive: Boolean = true): Seq[PipegraphModel]

  def insert(pipegraph: PipegraphModel): Unit

  def update(pipegraphModel: PipegraphModel): Unit

  def deleteById(id_string: String): Unit

  def setIsActive(pipegraphModel: PipegraphModel, isActive: Boolean): Unit = {
    pipegraphModel.isActive = isActive
    pipegraphModel.etl.foreach(etl => etl.isActive = isActive)
    pipegraphModel.rt.foreach(rt => rt.isActive = isActive)
    update(pipegraphModel)
  }
}


class PipegraphBLImp(waspDB: WaspDB) extends PipegraphBL {

  private def factory(p: PipegraphModel) = new PipegraphModel(p.name, p.description, p.owner, p.system, p.creationTime, p.etl, p.rt, p.dashboard, p.isActive, p._id)

  def getByName(name: String) = {
    waspDB.getDocumentByField[PipegraphModel]("name", new BsonString(name)).map(pipegraph => {
      factory(pipegraph)
    })
  }

  def getAll = {
    waspDB.getAll[PipegraphModel]
  }

  def getById(id: String) = {
    waspDB.getDocumentByID[PipegraphModel](BsonObjectId(id)).map(pipegraph => {
      factory(pipegraph)
    })
  }

  def getSystemPipegraphs(isSystemPipegraph: Boolean = true): Seq[PipegraphModel] = {
    waspDB.getAllDocumentsByField[PipegraphModel]("system", new BsonBoolean(isSystemPipegraph)).map(factory)
  }

  def getActivePipegraphs(isActive: Boolean = true) = {
    waspDB.getAllDocumentsByField[PipegraphModel]("isActive", new BsonBoolean(isActive)).map(factory)
  }
  def update(pipegraphModel: PipegraphModel): Unit = {
    waspDB.updateById[PipegraphModel](pipegraphModel._id.get, pipegraphModel)
  }

  def insert(pipegraph: PipegraphModel) = {
    waspDB.insertIfNotExists[PipegraphModel](pipegraph)
  }

  def deleteById(id_string: String) = {
    waspDB.deleteById[PipegraphModel](BsonObjectId(id_string))
  }

}