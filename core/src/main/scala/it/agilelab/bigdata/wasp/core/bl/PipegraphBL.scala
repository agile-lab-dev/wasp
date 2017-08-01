package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.{BSONConversionHelper, PipegraphModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import reactivemongo.bson.{BSONBoolean, BSONObjectID, BSONString}
import reactivemongo.api.commands.WriteResult

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait PipegraphBL  {

  def getByName(name: String): Future[Option[PipegraphModel]]

  def getById(id: String): Future[Option[PipegraphModel]]

  def getAll : Future[List[PipegraphModel]]

  def getSystemPipegraphs(isSystemPipegraph: Boolean = true): Future[List[PipegraphModel]]

  def getActivePipegraphs(isActive: Boolean = true): Future[List[PipegraphModel]]

  def insert(pipegraph: PipegraphModel): Future[WriteResult]

  def update(producerModel: PipegraphModel): Future[WriteResult]

  def deleteById(id_string: String): Future[WriteResult]

  def setIsActive(producerModel: PipegraphModel, isActive: Boolean): Future[WriteResult] = {
    producerModel.isActive = isActive
    producerModel.etl.foreach(etl => etl.isActive = isActive)
    producerModel.rt.foreach(rt => rt.isActive = isActive)
    update(producerModel)
  }
}


class PipegraphBLImp(waspDB: WaspDB) extends PipegraphBL with BSONConversionHelper {

  private def factory(p: PipegraphModel) = new PipegraphModel(p.name, p.description, p.owner, p.system, p.creationTime, p.etl, p.rt, p.dashboard, p.isActive, p._id)

  def getByName(name: String) = {
    waspDB.getDocumentByField[PipegraphModel]("name", new BSONString(name)).map(pipegraph => {
      pipegraph.map(p => factory(p))
    })
  }

  def getAll = {
    waspDB.getAll[PipegraphModel]
  }

  def getById(id: String) = {
    waspDB.getDocumentByID[PipegraphModel](BSONObjectID(id)).map(pipegraph => {
      pipegraph.map(p => factory(p))
    })
  }

  def getSystemPipegraphs(isSystemPipegraph: Boolean = true): Future[List[PipegraphModel]] = {
    waspDB.getAllDocumentsByField[PipegraphModel]("system", new BSONBoolean(isSystemPipegraph)).map(pipegraph => {
      pipegraph.map(p => factory(p))
    })
  }

  def getActivePipegraphs(isActive: Boolean = true) = {
    waspDB.getAllDocumentsByField[PipegraphModel]("isActive", new BSONBoolean(isActive)).map(pipegraph => {
      pipegraph.map(p => factory(p))
    })
  }
  def update(producerModel: PipegraphModel): Future[WriteResult] = {
    waspDB.updateById[PipegraphModel](producerModel._id.get, producerModel)
  }

  def insert(pipegraph: PipegraphModel) = {
    waspDB.insertIfNotExists[PipegraphModel](pipegraph)
  }

  def deleteById(id_string: String) = {
    waspDB.deleteById[PipegraphModel](BSONObjectID(id_string))
  }

}