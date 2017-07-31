package it.agilelab.bigdata.wasp.core.models

import org.json4s.JsonAST.JObject
import org.scalatest.WordSpec

class IndexModelBuilderSpec extends WordSpec {


  "An index model builder" must {

    "not compile until all required methods are called " in {


      import IndexModelBuilder._


      val builder = IndexModelBuilder.forSolr

      assertTypeError("builder.build")


      val withName = builder.named("index")

      assertTypeError("withName.build")

      val withConfig = withName.config(Solr.Config.default)

      assertTypeError("withConfig.build")

      //noinspection ScalaUnusedSymbol
      val withSchema = withConfig.schema(Solr.Schema())

      assertCompiles("withSchema.build")

    }

    "Correctly create a Solr IndexModel" in {


      import IndexModelBuilder._

      val solr = IndexModelBuilder.forSolr
                                  .named("test")
                                  .schema(Solr.Schema(
                                    Solr.Field("binaryField", Solr.Type.Binary),
                                    Solr.Field("booleanField", Solr.Type.Bool),
                                    Solr.Field("textFieldWithDefault", Solr.Type.Text, Some("default")),
                                    Solr.Field("intFieldWithSDefault", Solr.Type.TrieInt, Some(1))))
                                  .config(Solr.Config(shards = 3, replica = 4))
                                  .build


      val expectedSchema = "[" +
          "{\"name\":\"binaryField\",\"type\":\"binary\",\"indexed\":true,\"stored\":true,\"required\":false}," +
          "{\"name\":\"booleanField\",\"type\":\"boolean\",\"indexed\":true,\"stored\":true,\"required\":false}," +
          "{\"name\":\"textFieldWithDefault\",\"type\":\"text_general\",\"defaultValue\":\"default\",\"indexed\":true,\"stored\":true,\"required\":false}," +
          "{\"name\":\"intFieldWithSDefault\",\"type\":\"tint\",\"defaultValue\":1,\"indexed\":true,\"stored\":true,\"required\":false}" +
        "]"



      assertResult("test_index")(solr.name)
      assertResult(Some(3))(solr.numShards)
      assertResult(Some(4))(solr.replicationFactor)
      assertResult(false)(solr.rollingIndex)
      assertResult(None)(solr.idField)
      assertResult(Some(expectedSchema))(solr.schema)

    }

    "Correctly create an Elastic IndexModel" in {


      import IndexModelBuilder._

      val elastic = IndexModelBuilder.forElastic
                                     .named("test")
                                     .schema(Elastic.Schema(JObject()))
                                     .config(Elastic.Config(shards = 3, replica = 4))
                                     .build


      val expectedSchema = "{}"



      assertResult("test_index")(elastic.name)
      assertResult(Some(3))(elastic.numShards)
      assertResult(Some(4))(elastic.replicationFactor)
      assertResult(false)(elastic.rollingIndex)
      assertResult(None)(elastic.idField)
      assertResult(Some(expectedSchema))(elastic.schema)

    }

  }

}
