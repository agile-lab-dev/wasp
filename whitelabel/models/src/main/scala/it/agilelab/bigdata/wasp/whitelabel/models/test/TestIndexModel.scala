package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models.IndexModel
import it.agilelab.bigdata.wasp.core.utils.JsonConverter

private[wasp] object TestIndexModel {

  val index_name = IndexModel.normalizeName("test")

  def apply() = IndexModel(
    name = index_name,
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

  val indexSchemaSolr =
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
