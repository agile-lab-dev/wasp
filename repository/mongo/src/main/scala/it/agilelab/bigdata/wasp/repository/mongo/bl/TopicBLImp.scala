package it.agilelab.bigdata.wasp.repository.mongo.bl

import it.agilelab.bigdata.wasp.models._
import it.agilelab.bigdata.wasp.repository.core.bl.TopicBL
import it.agilelab.bigdata.wasp.repository.core.dbModels.{MultiTopicDBModel, MultiTopicDBModelV1, TopicDBModel, TopicDBModelV1}
import it.agilelab.bigdata.wasp.repository.core.mappers.MultiTopicModelMapperV1
import it.agilelab.bigdata.wasp.repository.core.mappers.TopicDBModelMapperSelector.applyMap
import it.agilelab.bigdata.wasp.repository.core.mappers.TopicMapperV1.transform
import it.agilelab.bigdata.wasp.repository.mongo.WaspMongoDB
import org.mongodb.scala.bson.{BsonDocument, BsonString}

import scala.collection.JavaConverters._

class TopicBLImp(waspDB: WaspMongoDB) extends TopicBL {

  private def factory(bsonDocument: BsonDocument): DatastoreModel = {
    if (bsonDocument.containsKey("partitions")) // TopicModel
      bsonDocumentToTopicModel(bsonDocument)
    else if (bsonDocument.containsKey("topicNameField")) // MultiTopicModel
      bsonDocumentToMultiTopicModel(bsonDocument)
    else // unknown
      throw new UnsupportedOperationException(
        s"Unsupported DatastoreModel[TopicCategory] encoded by BsonDocument: $bsonDocument"
      )
  }

  private def bsonDocumentToTopicModel(bsonDocument: BsonDocument): TopicModel = {
    new TopicModel(
      bsonDocument.get("name").asString().getValue,
      bsonDocument.get("creationTime").asInt64().getValue,
      bsonDocument.get("partitions").asInt32().getValue,
      bsonDocument.get("replicas").asInt32().getValue,
      bsonDocument.get("topicDataType").asString().getValue,
      if (bsonDocument.containsKey("keyFieldName") && !bsonDocument.get("keyFieldName").isNull)
        Some(bsonDocument.get("keyFieldName").asString().getValue)
      else
        None,
      if (bsonDocument.containsKey("headersFieldName") && !bsonDocument.get("headersFieldName").isNull)
        Some(bsonDocument.get("headersFieldName").asString().getValue)
      else
        None,
      if (bsonDocument.containsKey("valueFieldsNames") && !bsonDocument.get("valueFieldsNames").isNull)
        Some(bsonDocument.getArray("valueFieldsNames").getValues.asScala.toList.map(_.asString().getValue))
      else
        None,
      bsonDocument.get("useAvroSchemaManager").asBoolean().getValue,
      bsonDocument.get("schema").asDocument(),
      TopicCompression.fromString(bsonDocument.get("topicCompression").asString().getValue),
      SubjectStrategy.fromString(bsonDocument.get("subjectStrategy").asString().getValue),
      if (bsonDocument.containsKey("keySchema") && !bsonDocument.get("keySchema").isNull)
        Some(bsonDocument.get("keySchema").asString().getValue)
      else
        None
    )
  }

  private def bsonDocumentToMultiTopicModel(bsonDocument: BsonDocument): MultiTopicModel = {
    new MultiTopicModel(
      bsonDocument.get("name").asString().getValue,
      bsonDocument.get("topicNameField").asString().getValue,
      bsonDocument.getArray("topicModelNames").getValues.asScala.toList.map(_.asString().getValue)
    )
  }

  override def getByName(name: String): Option[DatastoreModel] = {
    // the type argument to getDocumentByFieldRaw is only used for collection lookup, so using TopicModel is fine
    waspDB
      .getDocumentByFieldRaw[TopicModel]("name", new BsonString(name)).map(factory)
  }

  override def getAll: Seq[DatastoreModel] = {
    // the type argument to getAllRaw is only used for collection lookup, so using TopicModel is fine

    waspDB.getAllRaw[TopicModel]().map(factory)
  }

  // drop encoders and decoder?

  override def persist(topicDatastoreModel: DatastoreModel): Unit = topicDatastoreModel match {
    case topicModel: TopicModel           => waspDB.insert[TopicDBModel](transform[TopicDBModelV1](topicModel))
    case multiTopicModel: MultiTopicModel => waspDB.insert[MultiTopicDBModel](MultiTopicModelMapperV1.transform[MultiTopicDBModelV1](multiTopicModel))
    case tdm                              => throw new UnsupportedOperationException(s"Unsupported DatastoreModel[TopicCategory]: $tdm")

  }

  override def insertIfNotExists(topicDatastoreModel: DatastoreModel): Unit = topicDatastoreModel match {
    case topicModel: TopicModel           => waspDB.insertIfNotExists[TopicDBModel](transform[TopicDBModelV1](topicModel))
    case multiTopicModel: MultiTopicModel => waspDB.insertIfNotExists[MultiTopicDBModel](MultiTopicModelMapperV1.transform[MultiTopicDBModelV1](multiTopicModel))
    case tdm                              => throw new UnsupportedOperationException(s"Unsupported DatastoreModel[TopicCategory]: $tdm")

  }

  override def upsert(topicDatastoreModel: DatastoreModel): Unit = topicDatastoreModel match {
    case topicModel: TopicModel           => waspDB.upsert[TopicDBModel](transform[TopicDBModelV1](topicModel))
    case multiTopicModel: MultiTopicModel => waspDB.upsert[MultiTopicDBModel](MultiTopicModelMapperV1.transform[MultiTopicDBModelV1](multiTopicModel))
    case tdm                              => throw new UnsupportedOperationException(s"Unsupported DatastoreModel[TopicCategory]: $tdm")

  }
}
