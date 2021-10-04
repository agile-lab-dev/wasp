package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.{PipegraphBL, PipegraphInstanceBl}
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel, PipegraphStatus}
import it.agilelab.bigdata.wasp.models.PipegraphStatus.PipegraphStatus
import it.agilelab.bigdata.wasp.repository.core.dbModels.{PipegraphDBModel, PipegraphInstanceDBModel}
import it.agilelab.bigdata.wasp.repository.core.mappers.PipegraphMapperV1.fromModelToDBModel
import it.agilelab.bigdata.wasp.repository.core.mappers.PipegraphInstanceMapperV1
import it.agilelab.bigdata.wasp.repository.core.mappers.PipegraphInstanceDBModelMapperSelector
import it.agilelab.bigdata.wasp.repository.core.mappers.PipegraphDBModelMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.{BsonBoolean, BsonDocument, BsonInt64, BsonString}

class PipegraphBLImp(waspDB: WaspMongoDB) extends PipegraphBL {

  def getByName(name: String): Option[PipegraphModel] = {
    waspDB
      .getDocumentByField[PipegraphDBModel]("name", new BsonString(name)).map(applyMap)
  }


  def getAll: Seq[PipegraphModel] = {
    waspDB.getAll[PipegraphDBModel].map(applyMap)
  }

  def getSystemPipegraphs: Seq[PipegraphModel] = {
    waspDB.getAllDocumentsByField[PipegraphDBModel]("isSystem", new BsonBoolean(true)).map(applyMap)
  }

  def getByOwner(owner: String): Seq[PipegraphModel] = {
    waspDB.getAllDocumentsByField[PipegraphDBModel]("owner", new BsonString(owner)).map(applyMap)
  }

  def getNonSystemPipegraphs: Seq[PipegraphModel] = {
    waspDB.getAllDocumentsByField[PipegraphDBModel]("isSystem", new BsonBoolean(false)).map(applyMap)
  }

  def getActivePipegraphs(): Seq[PipegraphModel] = {
    val allowedStates: Set[PipegraphStatus] = Set(PipegraphStatus.PENDING, PipegraphStatus.PROCESSING)

    instances
      .all()
      .filter(instance => allowedStates.contains(instance.status))
      .flatMap(instance => getByName(instance.instanceOf))
  }

  def update(pipegraphModel: PipegraphModel): Unit = {
    waspDB.updateByName[PipegraphDBModel](pipegraphModel.name, fromModelToDBModel(pipegraphModel))
  }

  def insert(pipegraph: PipegraphModel): Unit = {
    waspDB.insertIfNotExists[PipegraphDBModel](fromModelToDBModel(pipegraph))
  }

  override def insertIfNotExists(pipegraph: PipegraphModel): Unit =
    waspDB.insertIfNotExists[PipegraphDBModel](fromModelToDBModel(pipegraph))

  def upsert(pipegraph: PipegraphModel): Unit = {
    waspDB.upsert[PipegraphDBModel](fromModelToDBModel(pipegraph))
  }

  def deleteByName(name: String): Unit = {
    waspDB.deleteByName[PipegraphDBModel](name)
  }

  lazy val instances: PipegraphInstanceBl = new PipegraphInstanceBlImp(waspDB)

}

// TODO: move everything to mongo level encoder/decoder

class PipegraphInstanceBlImp(waspDB: WaspMongoDB) extends PipegraphInstanceBl {
  override def update(instance: PipegraphInstanceModel): PipegraphInstanceModel = {
    waspDB.updateByName[PipegraphInstanceDBModel](instance.name, PipegraphInstanceMapperV1.fromModelToDBModel(instance))
    instance
  }

  override def all(): Seq[PipegraphInstanceModel] =
    waspDB.getAll[PipegraphInstanceDBModel].map(PipegraphInstanceDBModelMapperSelector.applyMap)


  override def instancesOf(name: String): Seq[PipegraphInstanceModel] =
    waspDB.getAllDocumentsByField[PipegraphInstanceDBModel]("instanceOf", BsonString(name))
      .map(PipegraphInstanceDBModelMapperSelector.applyMap)

  override def insert(instance: PipegraphInstanceModel): PipegraphInstanceModel = {
    waspDB.insert[PipegraphInstanceDBModel](PipegraphInstanceMapperV1.fromModelToDBModel(instance))
    instance
  }

  override def getByName(name: String): Option[PipegraphInstanceModel] = {
    waspDB.getDocumentByField[PipegraphInstanceDBModel]("name", new BsonString(name))
      .map(PipegraphInstanceDBModelMapperSelector.applyMap)
  }
}
