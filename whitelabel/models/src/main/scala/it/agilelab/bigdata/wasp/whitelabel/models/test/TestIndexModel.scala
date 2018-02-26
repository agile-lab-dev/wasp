package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.utils.JsonConverter


private[wasp] object TestIndexModel {

  import org.json4s._

  import org.json4s.native.JsonMethods._
  import org.json4s.JsonDSL._

  lazy val solr = IndexModel(
    name = IndexModel.normalizeName("test_solr"),
    creationTime = System.currentTimeMillis,
    // Solr
    schema = JsonConverter.fromString(indexSchemaSolr),
    query = None,
    // Work only with Solr
    numShards = Some(1),
    // Work only with Solr
    replicationFactor = Some(1),
    rollingIndex = false
  )

  private lazy val indexSchemaSolr =
    """
      { "properties":
        [
          { "name":"number", "type":"tint", "stored":true },
          { "name":"nested.field1", "type":"string", "stored":true },
          { "name":"nested.field2", "type":"tlong", "stored":true },
          { "name":"nested.field3", "type":"string", "stored":true, "required":false }
        ]
      }
    """

  lazy val elastic = IndexModel(
    name = IndexModel.normalizeName("test_elastic"),
    creationTime = System.currentTimeMillis,
    schema = JsonConverter.fromString(compact(render(indexElasticSchema))),
    query = None,
    numShards = Some(1),
    replicationFactor = Some(1),
    rollingIndex = false,
    idField = Some("id")
  )

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
