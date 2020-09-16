package it.agilelab.bigdata.wasp.whitelabel.models.example.iot

import java.time.{ZoneOffset, ZonedDateTime}
import java.time.format.DateTimeFormatter
import java.util.{Date, UUID}

import com.sksamuel.avro4s.AvroSchema
import it.agilelab.bigdata.wasp.core.utils.JsonConverter
import it.agilelab.bigdata.wasp.models.IndexModelBuilder.Solr
import it.agilelab.bigdata.wasp.models.{IndexModel, IndexModelBuilder, TopicModel}

import scala.util.Random

object IoTIndustrialPlantIndexModel {

    val index_name = "industrial_plant_solr"

    import IndexModelBuilder._

    def apply(): IndexModel =
      IndexModelBuilder.forSolr
        .named(index_name)
        .config(Solr.Config(shards = 1, replica = 1))
        .schema(
          Solr.Schema(
            Solr.Field("site", Solr.Type.String),
            Solr.Field("plant", Solr.Type.String),
            Solr.Field("line", Solr.Type.String),
            Solr.Field("machine", Solr.Type.String),
            Solr.Field("areadId", Solr.Type.String),
            Solr.Field("timestamp", Solr.Type.TrieDate),
            Solr.Field("kpi", Solr.Type.String),
            Solr.Field("kpiValue", Solr.Type.TrieInt)
          )
        )
        .build

}

