package it.agilelab.bigdata.wasp.repository.mongo

import it.agilelab.bigdata.wasp.models.configuration._
import it.agilelab.bigdata.wasp.repository.mongo.bl.ConfigManagerBLImpl
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers}


@DoNotDiscover
class ConfigManagerBLImplTest extends FlatSpec with Matchers{

  it should "test ConfigManagerBL" in {
    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val bl = new ConfigManagerBLImpl(waspDB)

    val solr = SolrConfigModel(ZookeeperConnectionsConfig(Seq(), "name"), "solr")
    val hbase = HBaseConfigModel("hbase", "hbaseXML", Seq(), "hbase")
    val kafka = KafkaConfigModel(
      Seq(),
      "ingest_rate",
      ZookeeperConnectionsConfig(Seq(), "name"),
      "broker",
      "partitioner",
      "encoder",
      "key",
      "encoderfq",
      "decoderfq",
      50,
      "acks",
      Seq(),
      "kafka"
    )

    val jdbc = JdbcConfigModel(Map("x" -> JdbcConnectionConfig("name", "url", "user" ,"password","driver")),"jdbc")
    val telemetry = TelemetryConfigModel("telemetry", "twriter", 1, TelemetryTopicConfigModel("test_topic", 2,3,Seq(), Seq(JMXTelemetryConfigModel("query", "metci", "source"))))


    val solrDB = bl.retrieveConf[SolrConfigModel](solr, "solr").get
    val hbaseDB = bl.retrieveConf[HBaseConfigModel](hbase, "hbase").get
    val kafkaDB = bl.retrieveConf[KafkaConfigModel](kafka, "kafka").get
    val jdbcDB = bl.retrieveConf[JdbcConfigModel](jdbc, "jdbc").get
    val telemetryDB = bl.retrieveConf[TelemetryConfigModel](telemetry, "telemetry").get


    solrDB shouldBe solr
    hbaseDB shouldBe hbase
    kafkaDB shouldBe kafka
    jdbcDB shouldBe jdbc
    telemetryDB shouldBe telemetry
  }

}
