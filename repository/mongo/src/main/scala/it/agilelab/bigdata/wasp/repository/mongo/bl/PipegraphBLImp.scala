package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.repository.core.bl.{PipegraphBL, PipegraphInstanceBl}
import it.agilelab.bigdata.wasp.models.{PipegraphInstanceModel, PipegraphModel, PipegraphStatus}
import it.agilelab.bigdata.wasp.models.PipegraphStatus.PipegraphStatus
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.{BsonBoolean, BsonDocument, BsonInt64, BsonString}

class PipegraphBLImp(waspDB: WaspMongoDB) extends PipegraphBL {

  def getByName(name: String): Option[PipegraphModel] = {
    waspDB
      .getDocumentByField[PipegraphModel]("name", new BsonString(name))
      .map(pipegraph => {
        factory(pipegraph)
      })
  }

  private def factory(p: PipegraphModel) =
    PipegraphModel(
      p.name,
      p.description,
      p.owner,
      p.isSystem,
      p.creationTime,
      p.legacyStreamingComponents,
      p.structuredStreamingComponents,
      p.rtComponents,
      p.dashboard
    )

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

  override def insertIfNotExists(pipegraph: PipegraphModel): Unit = waspDB.insertIfNotExists[PipegraphModel](pipegraph)

  def upsert(pipegraph: PipegraphModel): Unit = {
    waspDB.upsert[PipegraphModel](pipegraph)
  }

  def deleteByName(name: String): Unit = {
    waspDB.deleteByName[PipegraphModel](name)
  }

  lazy val instances = new PipegraphInstanceBlImp(waspDB)

}

class PipegraphInstanceBlImp(waspDB: WaspMongoDB) extends PipegraphInstanceBl {
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

    val withError = instance.error
      .map(error => document.append("error", BsonString(error)))
      .getOrElse(document)

    val withExecuted = instance.executedByNode
      .map(node => document.append("executedByNode", BsonString(node)))
      .getOrElse(withError)

    val withPeer = instance.peerActor
      .map(peer => document.append("peerActor", BsonString(peer)))
      .getOrElse(withExecuted)

    withPeer
  }

  override def all(): Seq[PipegraphInstanceModel] =
    waspDB.getAllRaw[PipegraphInstanceModel].map(decode)

  private def decode(bsonDocument: BsonDocument): PipegraphInstanceModel =
    PipegraphInstanceModel(
      name = bsonDocument.get("name").asString().getValue,
      instanceOf = bsonDocument.get("instanceOf").asString().getValue,
      startTimestamp = bsonDocument.get("startTimestamp").asInt64().getValue,
      currentStatusTimestamp = bsonDocument.get("currentStatusTimestamp").asInt64().getValue,
      status = PipegraphStatus.withName(bsonDocument.get("status").asString().getValue),
      executedByNode =
        if (bsonDocument.containsKey("executedByNode")) Some(bsonDocument.get("executedByNode").asString().getValue)
        else None,
      peerActor =
        if (bsonDocument.containsKey("peerActor")) Some(bsonDocument.get("peerActor").asString().getValue)
        else None,
      error = if (bsonDocument.containsKey("error")) Some(bsonDocument.get("error").asString().getValue) else None
    )

  override def instancesOf(name: String): Seq[PipegraphInstanceModel] =
    waspDB.getAllDocumentsByFieldRaw[PipegraphInstanceModel]("instanceOf", BsonString(name)).map(decode)

  override def insert(instance: PipegraphInstanceModel): PipegraphInstanceModel = {
    waspDB.insertRaw[PipegraphInstanceModel](encode(instance))
    instance
  }

  override def getByName(name: String): Option[PipegraphInstanceModel] = {
    waspDB.getDocumentByFieldRaw[PipegraphInstanceModel]("name", new BsonString(name)).map(decode)
  }
}
