package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.models.{IndexModel, IndexModelBuilder}

import it.agilelab.bigdata.wasp.core.utils.JsonConverter

private[wasp] object TestIndexModel {

  import org.json4s._

  import org.json4s.native.JsonMethods._
  import org.json4s.JsonDSL._

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


  def main(args: Array[String]): Unit = {
    println(pretty(render(indexElasticSchema)))
  }
}
