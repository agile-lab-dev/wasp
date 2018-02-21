package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.utils.JsonConverter

private[wasp] object TestIndexModel {

  private val index_name = "test"

  lazy val solr = IndexModel(
    name = IndexModel.normalizeName(index_name),
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
          { "name":"nested.field1", "type" : "string", "stored" : true },
          { "name":"nested.field2", "type":"tlong", "stored":true },
          { "name":"nested.field3", "type" : "string", "stored" : true, "required":false }
        ]
      }"""
}
