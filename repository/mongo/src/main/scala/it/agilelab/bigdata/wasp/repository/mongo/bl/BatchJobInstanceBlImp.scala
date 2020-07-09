package it.agilelab.bigdata.wasp.repository.mongo.bl

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import it.agilelab.bigdata.wasp.repository.core.bl.BatchJobInstanceBL
import it.agilelab.bigdata.wasp.models.{BatchJobInstanceModel, JobStatus}
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.{BsonDocument, BsonInt64, BsonString}

class BatchJobInstanceBlImp(waspDB: WaspMongoDB) extends BatchJobInstanceBL {
  override def update(instance: BatchJobInstanceModel): BatchJobInstanceModel = {
    waspDB.updateByNameRaw[BatchJobInstanceModel](instance.name, encode(instance))
    instance
  }

  override def all(): Seq[BatchJobInstanceModel] =
    waspDB.getAllRaw[BatchJobInstanceModel].map(decode)

  override def instancesOf(name: String): Seq[BatchJobInstanceModel] =
    waspDB.getAllDocumentsByFieldRaw[BatchJobInstanceModel]("instanceOf", BsonString(name)).map(decode)

  override def insert(instance: BatchJobInstanceModel): BatchJobInstanceModel = {
    waspDB.insertRaw[BatchJobInstanceModel](encode(instance))
    instance
  }

  override def getByName(name: String): Option[BatchJobInstanceModel] = {
    waspDB.getDocumentByFieldRaw[BatchJobInstanceModel]("name", new BsonString(name)).map(decode)
  }

  private def decode(bsonDocument: BsonDocument) : BatchJobInstanceModel =
    BatchJobInstanceModel(name = bsonDocument.get("name").asString().getValue,
      instanceOf = bsonDocument.get("instanceOf").asString().getValue,
      startTimestamp = bsonDocument.get("startTimestamp").asInt64().getValue,
      currentStatusTimestamp = bsonDocument.get("currentStatusTimestamp").asInt64().getValue,
      status = JobStatus.withName(bsonDocument.get("status").asString().getValue),
      restConfig = ConfigFactory.parseString(bsonDocument.get("restConfig").asString.getValue),
      error = if (bsonDocument.containsKey("error")) Some(bsonDocument.get("error").asString().getValue) else None)


  private def encode(instance: BatchJobInstanceModel): BsonDocument = {
    val document = BsonDocument()
      .append("name", BsonString(instance.name))
      .append("instanceOf", BsonString(instance.instanceOf))
      .append("startTimestamp", BsonInt64(instance.startTimestamp))
      .append("currentStatusTimestamp", BsonInt64(instance.currentStatusTimestamp))
      .append("status", BsonString(instance.status.toString))
      .append("restConfig", BsonString(instance.restConfig.root.render(ConfigRenderOptions.concise())))


    val withError = instance.error.map(error => document.append("error", BsonString(error)))
      .getOrElse(document)

    return withError
  }
}
