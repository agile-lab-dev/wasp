package it.agilelab.bigdata.wasp.repository.mongo

import it.agilelab.bigdata.wasp.models.{CdcModel, CdcOptions}
import it.agilelab.bigdata.wasp.repository.mongo.bl.CdcBLImp
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.scalatest.{DoNotDiscover, FlatSpec, Matchers}

@DoNotDiscover
class CdcBLImplTest extends FlatSpec with Matchers{

  it should "test cdcBLImpl for Mongo" in {
    val db = WaspMongoDB
    db.initializeDB()
    val waspDB = db.getDB()
    val cdcImp = new CdcBLImp(waspDB)

    val debeziumMutation = CdcModel(
      name = "TestDemeziumMutationModel",
      uri = "hdfs://localhost:9000/tmp/mutations-delta-table",
      schema = StructType(
        Seq(
          StructField("id", IntegerType)
      )).json,
      options = CdcOptions.defaultAppend
    )

    cdcImp.persist(debeziumMutation)
    cdcImp.getByName("TestDemeziumMutationModel") shouldBe Some(debeziumMutation)

    cdcImp.getAll should contain theSameElementsAs Seq(debeziumMutation)




  }
}
