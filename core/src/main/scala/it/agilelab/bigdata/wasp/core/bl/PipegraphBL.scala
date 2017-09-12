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
  
  def getSystemPipegraphs: Seq[PipegraphModel]

  def getNonSystemPipegraphs: Seq[PipegraphModel]

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

  private def factory(p: PipegraphModel) = PipegraphModel(p.name, p.description, p.owner, p.isSystem, p.creationTime, p.etl, p.rt, p.dashboard, p.isActive, p._id)

  def getByName(name: String): Option[PipegraphModel] = {
    waspDB.getDocumentByField[PipegraphModel]("name", new BsonString(name)).map(pipegraph => {
      factory(pipegraph)
    })
  }

  def getAll: Seq[PipegraphModel] = {
    waspDB.getAll[PipegraphModel]
  }

  def getById(id: String): Option[PipegraphModel] = {
    waspDB.getDocumentByID[PipegraphModel](BsonObjectId(id)).map(pipegraph => {
      factory(pipegraph)
    })
  }

  def getSystemPipegraphs: Seq[PipegraphModel] = {
    waspDB.getAllDocumentsByField[PipegraphModel]("isSystem", new BsonBoolean(true)).map(factory)
  }
  
  def getNonSystemPipegraphs: Seq[PipegraphModel] = {
    waspDB.getAllDocumentsByField[PipegraphModel]("isSystem", new BsonBoolean(false)).map(factory)
  }

  def getActivePipegraphs(isActive: Boolean = true): Seq[PipegraphModel] = {
    waspDB.getAllDocumentsByField[PipegraphModel]("isActive", new BsonBoolean(isActive)).map(factory)
  }
  def update(pipegraphModel: PipegraphModel): Unit = {
    waspDB.updateById[PipegraphModel](pipegraphModel._id.get, pipegraphModel)
  }

  def insert(pipegraph: PipegraphModel): Unit = {
    waspDB.insertIfNotExists[PipegraphModel](pipegraph)
  }

  def deleteById(id_string: String): Unit = {
    waspDB.deleteById[PipegraphModel](BsonObjectId(id_string))
  }

}