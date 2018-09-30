package it.agilelab.bigdata.wasp.core.models

import it.agilelab.bigdata.wasp.core.datastores.TopicCategory

/**
	* @author Nicolò Bidotti
	*/
case class MultiTopicModel private[wasp] (override val name: String,
                                          topicNameField: String,
                                          topicModelNames: Seq[String])
	extends DatastoreModel[TopicCategory]

object MultiTopicModel {
	def fromTopicModels(name: String, topicNameField: String, topicModels: Seq[TopicModel]): MultiTopicModel = {
		validateTopicModels(topicModels)
		new MultiTopicModel(name, topicNameField, topicModels.map(_.name))
	}
	
	/**
		* Checks that the topic models:
		* - are all different models
		* - refer to different topics
		* - use the same settings for everthing but partitions and replicas 
		*/
	private[wasp] def validateTopicModels(models: Seq[TopicModel]): Unit = {
		require(models.size == models.distinct.size, "Each topic model can only appear once")
		require(models.size == models.map(_.name).distinct.size, "Each topic can only appear once")
		require(models.map(_.topicDataType).distinct.length == 1, "All topic models must have the same topic data type")
		require(models.map(_.keyFieldName).distinct.length == 1, "All topic models must have the same key field name")
		require(models.map(_.headersFieldName).distinct.length == 1, "All topic models must have the same headers field name")
		require(models.map(_.valueFieldsNames).distinct.length == 1, "All topic models must have the same value field names")
		require(models.map(_.getJsonSchema).distinct.size == 1, "All topic models must have the same schema")
	}
}
