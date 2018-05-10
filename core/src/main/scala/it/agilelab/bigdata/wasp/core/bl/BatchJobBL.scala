package it.agilelab.bigdata.wasp.core.bl

import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import it.agilelab.bigdata.wasp.core.models.{BatchJobInstanceModel, BatchJobModel, JobStatus}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.mongodb.scala.bson.{BsonDocument, BsonInt64, BsonString}

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

  def deleteByName(name: String): Unit

  def instances(): BatchJobInstanceBL

}


class BatchJobInstanceBlImp(waspDB: WaspDB) extends BatchJobInstanceBL {
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


class BatchJobBLImp(waspDB: WaspDB) extends BatchJobBL {

  val instanceBl = new BatchJobInstanceBlImp(waspDB)

  def deleteByName(name: String): Unit = {
    waspDB.deleteByName(name)
  }

  def getAll: Seq[BatchJobModel] = {
    waspDB.getAll[BatchJobModel]().map(factory)
  }

  def update(batchJobModel: BatchJobModel): Unit = {
    waspDB.updateByName[BatchJobModel](batchJobModel.name, batchJobModel)
  }

  def insert(batchJobModel: BatchJobModel): Unit = {
    waspDB.insertIfNotExists[BatchJobModel](batchJobModel)
  }

  def getByName(name: String): Option[BatchJobModel] = {
    waspDB.getDocumentByField[BatchJobModel]("name", new BsonString(name)).map(batchJob => {
      factory(batchJob)
    })
  }

  private def factory(p: BatchJobModel) = new BatchJobModel(p.name, p.description, p.owner, p.system, p.creationTime, p.etl, p.exclusivityConfig)

  override def instances(): BatchJobInstanceBL = instanceBl


}