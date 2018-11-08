package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.datastores.TopicCategory
import it.agilelab.bigdata.wasp.core.models.{DatastoreModel, MultiTopicModel, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.WaspDB
import org.mongodb.scala.bson.{BsonDocument, BsonString}

import scala.collection.JavaConverters._


trait TopicBL {

  def getByName(name: String): Option[DatastoreModel[TopicCategory]]
	
	/**
		* Gets a TopicModel by name; an exception is thrown if a MultiTopicModel or anything else is found instead.
		*/
	@throws[Exception]
	def getTopicModelByName(name: String): Option[TopicModel] = {
		getByName(name) map {
			case topicModel: TopicModel => topicModel
			case multiTopicModel: MultiTopicModel =>
				throw new Exception(s"Found MultiTopicModel instead of TopicModel for name $name")
		}
	}

  def getAll : Seq[DatastoreModel[TopicCategory]]

  def persist(topicModel: DatastoreModel[TopicCategory]): Unit

}

class TopicBLImp(waspDB: WaspDB) extends TopicBL  {

  private def factory(bsonDocument: BsonDocument): DatastoreModel[TopicCategory] = {
    if (bsonDocument.containsKey("partitions")) // TopicModel
      bsonDocumentToTopicModel(bsonDocument)
    else if (bsonDocument.containsKey("topicNameField")) // MultiTopicModel
      bsonDocumentToMultiTopicModel(bsonDocument)
    else // unknown
      throw new UnsupportedOperationException(s"Unsupported DatastoreModel[TopicCategory] encoded by BsonDocument: $bsonDocument")
  }

  private def bsonDocumentToTopicModel(bsonDocument: BsonDocument): TopicModel = {
    new TopicModel(bsonDocument.get("name").asString().getValue,
                   bsonDocument.get("creationTime").asInt64().getValue,
                   bsonDocument.get("partitions").asInt32().getValue,
                   bsonDocument.get("replicas").asInt32().getValue,
                   bsonDocument.get("topicDataType").asString().getValue,
                   if (bsonDocument.containsKey("keyFieldName"))
                     Some(bsonDocument.get("keyFieldName").asString().getValue)
                   else
                     None,
                   if (bsonDocument.containsKey("headersFieldName"))
                     Some(bsonDocument.get("headersFieldName").asString().getValue)
                   else
                     None,
                   if (bsonDocument.containsKey("valueFieldsNames"))
                     Some(bsonDocument.getArray("valueFieldsNames").getValues.asScala.toList.map(_.asString().getValue))
                   else
                     None,
                   bsonDocument.get("useAvroSchemaManager").asBoolean().getValue,
                   bsonDocument.get("schema").asDocument())
  }
  
  private def bsonDocumentToMultiTopicModel(bsonDocument: BsonDocument): MultiTopicModel = {
    new MultiTopicModel(bsonDocument.get("name").asString().getValue,
                        bsonDocument.get("topicNameField").asString().getValue,
                        bsonDocument.getArray("topicModelNames").getValues.asScala.toList.map(_.asString().getValue))
  }
  
  override def getByName(name: String): Option[DatastoreModel[TopicCategory]] = {
	  // the type argument to getDocumentByFieldRaw is only used for collection lookup, so using TopicModel is fine
	  waspDB.getDocumentByFieldRaw[TopicModel]("name", new BsonString(name)).map(topic => {
		  factory(topic)
	  })
  }

  override def getAll: Seq[DatastoreModel[TopicCategory]] = {
    // the type argument to getAllRaw is only used for collection lookup, so using TopicModel is fine
    waspDB.getAllRaw[TopicModel]().map(factory)
  }

  override def persist(topicDatastoreModel: DatastoreModel[TopicCategory]): Unit = topicDatastoreModel match {
    case topicModel: TopicModel => waspDB.insert[TopicModel](topicModel)
    case multiTopicModel: MultiTopicModel => waspDB.insert[MultiTopicModel](multiTopicModel)
    case tdm => throw new UnsupportedOperationException(s"Unsupported DatastoreModel[TopicCategory]: $tdm")
  
  }
}