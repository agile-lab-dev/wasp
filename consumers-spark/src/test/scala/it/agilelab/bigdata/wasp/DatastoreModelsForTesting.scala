package it.agilelab.bigdata.wasp

import it.agilelab.bigdata.wasp.core.models.{IndexModel, IndexModelBuilder, TopicModel}
import it.agilelab.bigdata.wasp.core.utils.JsonConverter

/**
	* Datastore models for use in testing code.
	*
	* @author Nicolò Bidotti
	*/
object DatastoreModelsForTesting {
	object IndexModels {
     import IndexModelBuilder._
     import org.json4s.JsonDSL._
     import org.json4s._

     lazy val solr: IndexModel = IndexModelBuilder.forSolr
       .named("test_solr")
       .config(Solr.Config.default)
       .schema(Solr.Schema(
         Solr.Field("number", Solr.Type.TrieInt),
         Solr.Field("nested.field1", Solr.Type.String),
         Solr.Field("nested.field2", Solr.Type.TrieLong),
         Solr.Field("nested.field3", Solr.Type.String)))
       .build

     lazy val elastic: IndexModel = IndexModelBuilder.forElastic
       .named("test_elastic")
       .config(Elastic.Config.default)
       .schema(Elastic.Schema(indexElasticSchema))
       .build

     //noinspection ScalaUnnecessaryParentheses
     private lazy val indexElasticSchema = JObject(
				("properties" ->
					("id" ->
						("type" -> "keyword")
						) ~
						("number" ->
							("type" -> "integer")
							) ~
						("nested" ->
							("properties" ->
								("field1" ->
									("type" -> "text")
									) ~
									("field2" ->
										("type" -> "long")
										) ~
									("field3" ->
										("type" -> "text")
										)
								)
							)
					)
			)
	}

	object TopicModels {
		private val topic_name = "test"

		private val topicSchema =
			TopicModel.generateField("test", "test", Some(
				"""
					|        {
					|            "name": "id",
					|            "type": "string",
					|            "doc": ""
					|        },
					|        {
					|            "name": "number",
					|            "type": "int",
					|            "doc": ""
					|        }
				""".stripMargin))

		lazy val json = TopicModel(name = TopicModel.name(topic_name + "_json"),
		                           creationTime = System.currentTimeMillis,
		                           partitions = 3,
		                           replicas = 1,
		                           topicDataType = "json",
		                           keyFieldName = None,
		                           headersFieldName = None,
		                           valueFieldsNames = None,
		                           schema = JsonConverter
			                           .fromString(topicSchema)
			                           .getOrElse(org.mongodb.scala.bson.BsonDocument()),
			                         useAvroSchemaManager = false)
	}
}
