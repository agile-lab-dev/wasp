package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.models.PipegraphStatus.PipegraphStatus
import it.agilelab.bigdata.wasp.core.models.{PipegraphInstanceModel, PipegraphModel, PipegraphStatus}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.mongodb.scala.bson.{BsonBoolean, BsonDocument, BsonInt64, BsonString}


trait PipegraphInstanceBl {
  def insert(instance: PipegraphInstanceModel): PipegraphInstanceModel

  def update(instance: PipegraphInstanceModel): PipegraphInstanceModel

  def all(): Seq[PipegraphInstanceModel]

  def instancesOf(name: String): Seq[PipegraphInstanceModel]
}

class PipegraphInstanceBlImp(waspDB: WaspDB) extends PipegraphInstanceBl {
  override def update(instance: PipegraphInstanceModel): PipegraphInstanceModel = {
    waspDB.updateByNameRaw[PipegraphInstanceModel](instance.name, encode(instance))
    instance
  }

  private def encode(instance: PipegraphInstanceModel): BsonDocument = {
    val document = BsonDocument()
      .append("name", BsonString(instance.name))
      .append("instanceOf", BsonString(instance.instanceOf))
      .append("startTimestamp", BsonInt64(instance.startTimestamp))
      .append("currentStatusTimestamp", BsonInt64(instance.currentStatusTimestamp))
      .append("status", BsonString(instance.status.toString))


    val withError = instance.error.map(error => document.append("error", BsonString(error)))
      .getOrElse(document)

    return withError
  }

  override def all(): Seq[PipegraphInstanceModel] =
    waspDB.getAllRaw[PipegraphInstanceModel].map(decode)

  private def decode(bsonDocument: BsonDocument): PipegraphInstanceModel =
    PipegraphInstanceModel(name = bsonDocument.get("name").asString().getValue,
      instanceOf = bsonDocument.get("instanceOf").asString().getValue,
      startTimestamp = bsonDocument.get("startTimestamp").asInt64().getValue,
      currentStatusTimestamp = bsonDocument.get("currentStatusTimestamp").asInt64().getValue,
      status = PipegraphStatus.withName(bsonDocument.get("status").asString().getValue),
      error = if (bsonDocument.containsKey("error")) Some(bsonDocument.get("error").asString().getValue) else None)

  override def instancesOf(name: String): Seq[PipegraphInstanceModel] =
    waspDB.getAllDocumentsByFieldRaw[PipegraphInstanceModel]("instanceOf", BsonString(name)).map(decode)

  override def insert(instance: PipegraphInstanceModel): PipegraphInstanceModel = {
    waspDB.insertRaw[PipegraphInstanceModel](encode(instance))
    instance
  }

}

trait PipegraphBL {

  def getByName(name: String): Option[PipegraphModel]

  def getAll: Seq[PipegraphModel]

  def getSystemPipegraphs: Seq[PipegraphModel]

  def getNonSystemPipegraphs: Seq[PipegraphModel]

  def getActivePipegraphs(): Seq[PipegraphModel]

  def insert(pipegraph: PipegraphModel): Unit

  def update(pipegraphModel: PipegraphModel): Unit

  def deleteByName(id_string: String): Unit

  def instances(): PipegraphInstanceBl

}


class PipegraphBLImp(waspDB: WaspDB) extends PipegraphBL {

  def getByName(name: String): Option[PipegraphModel] = {
    waspDB.getDocumentByField[PipegraphModel]("name", new BsonString(name)).map(pipegraph => {
      factory(pipegraph)
    })
  }

  private def factory(p: PipegraphModel) = PipegraphModel(p.name, p.description, p.owner, p.isSystem, p.creationTime, p.legacyStreamingComponents, p.structuredStreamingComponents, p.rtComponents, p.dashboard)

  def getAll: Seq[PipegraphModel] = {
    waspDB.getAll[PipegraphModel]
  }


  def getSystemPipegraphs: Seq[PipegraphModel] = {
    waspDB.getAllDocumentsByField[PipegraphModel]("isSystem", new BsonBoolean(true)).map(factory)
  }

  def getNonSystemPipegraphs: Seq[PipegraphModel] = {
    waspDB.getAllDocumentsByField[PipegraphModel]("isSystem", new BsonBoolean(false)).map(factory)
  }

  def getActivePipegraphs(): Seq[PipegraphModel] = {
    val allowedStates: Set[PipegraphStatus] = Set(PipegraphStatus.PENDING, PipegraphStatus.PROCESSING)

    instances
      .all()
      .filter(instance => allowedStates.contains(instance.status))
      .flatMap(instance => getByName(instance.name))
  }

  def update(pipegraphModel: PipegraphModel): Unit = {
    waspDB.updateByName[PipegraphModel](pipegraphModel.name, pipegraphModel)
  }

  def insert(pipegraph: PipegraphModel): Unit = {
    waspDB.insertIfNotExists[PipegraphModel](pipegraph)
  }

  def deleteByName(name: String): Unit = {
    waspDB.deleteByName[PipegraphModel](name)
  }

  lazy val instances = new PipegraphInstanceBlImp(waspDB)

}