package it.agilelab.bigdata.wasp.core.bl

import it.agilelab.bigdata.wasp.core.datastores.TopicCategory
import it.agilelab.bigdata.wasp.core.models.{DatastoreModel, MultiTopicModel, TopicModel}

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

  def upsert(topicModel: DatastoreModel[TopicCategory]): Unit

  def insertIfNotExists(topicDatastoreModel: DatastoreModel[TopicCategory]): Unit

}