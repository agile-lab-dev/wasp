package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.models.{IndexModel, IndexModelBuilder}
import spray.json.{JsObject, JsString, JsValue}
import it.agilelab.bigdata.wasp.models.SpraySolrProtocol._


private[wasp] object TestIndexModel {
  import IndexModelBuilder._


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

  def main(args: Array[String]): Unit = {
    println(indexElasticSchema.prettyPrint)
  }
}
