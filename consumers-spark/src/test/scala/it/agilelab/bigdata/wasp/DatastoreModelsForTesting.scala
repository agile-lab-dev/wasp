package it.agilelab.bigdata.wasp

import it.agilelab.bigdata.wasp.core.utils.JsonConverter
import it.agilelab.bigdata.wasp.models.{IndexModel, IndexModelBuilder, TopicModel}
import it.agilelab.bigdata.wasp.models.SpraySolrProtocol._


/**
	* Datastore models for use in testing code.
	*
	* @author Nicolò Bidotti
	*/
object DatastoreModelsForTesting {
	object IndexModels {
     import it.agilelab.bigdata.wasp.models.IndexModelBuilder._
		 import spray.json._

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
		private lazy val indexElasticSchema: JsValue = JsObject(
			"properties" -> JsObject(
				"id" -> JsObject("type" -> JsString("keyword")),
				"number" -> JsObject("type" -> JsString("integer")),
				"nested" -> JsObject(
					"properties" -> JsObject(
						"field1" -> JsObject(
							"type" -> JsString("text")
						),
						"field2" -> JsObject(
							"type" -> JsString("long")
						),
						"field3" -> JsObject(
							"type" -> JsString("text")
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

		lazy val json: TopicModel = models.TopicModel(name = TopicModel.name(topic_name + "_json"),
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
