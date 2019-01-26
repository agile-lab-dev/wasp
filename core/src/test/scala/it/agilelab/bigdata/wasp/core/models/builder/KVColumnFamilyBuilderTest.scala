package it.agilelab.bigdata.wasp.core.models.builder

import it.agilelab.bigdata.wasp.core.models.builder.KVColumnFamily.ColumnFamilyBuilder
import org.scalatest.{FlatSpec, Matchers}


class KVColumnFamilyBuilderTest extends FlatSpec with Matchers {

  it should "not compile an incomplete columnFamily" in {
    assertDoesNotCompile(
      """ColumnFamilyBuilder
      .emptyColumnFamilyBuilder
      .withCellQualifier(CellQualifier.PrimitiveCellQualifier("field", "qualifier", KVType.LONG))
      .withCellQualifier(CellQualifier.AvroCellQualifier("structField", "q2", "avroSchema"))
      .build""")

    assertDoesNotCompile(
      """ColumnFamilyBuilder
      .emptyColumnFamilyBuilder
      .withName("myCf")
      .build""")
  }

  it should "build a complete not clustered column family" in {
    val cf = ColumnFamilyBuilder
      .withName("myCf")
      .withCellQualifier(KVColumn.primitive("field", "qualifier", KVType.LONG))
      .withCellQualifier(KVColumn.avro("structField", "q2", "avroSchema"))
      .build

    cf.name should be("myCf")
    cf.cellQualifiers should contain theSameElementsAs (
      KVColumn.primitive("field", "qualifier", KVType.LONG) ::
        KVColumn.avro("structField", "q2", "avroSchema") :: Nil
      )
    cf.toJson.mkString(",\n") should be(
      """
        |"field": {"cf": "myCf", "col": "qualifier", "type": "long"},
        |"structField": {"cf": "myCf", "col": "q2", "avro": "avroSchema"}""".stripMargin.trim()
    )
  }

  it should "build a complete clustered column family" in {
    val cf = ColumnFamilyBuilder
      .withName("myCf")
      .withCellQualifier(KVColumn.primitive("field", "qualifier", KVType.LONG))
      .withCellQualifier(KVColumn.avro("structField", "q2", "avroSchema"))
      .withClusteringColumn(KVColumn.primitive("ts", "timestamp", KVType.STRING))
      .withClusteringColumn(KVColumn.primitive("ts2", "timestamp2", KVType.STRING))
      .build

    cf.name should be("myCf")
    cf.cellQualifiers should contain theSameElementsAs (
      KVColumn.primitive("field", "qualifier", KVType.LONG) ::
        KVColumn.avro("structField", "q2", "avroSchema") :: Nil
      )
    cf.toJson.mkString(",\n") should be(
      """
        |"clustering": {"cf": "myCf", "columns": "timestamp:timestamp2"},
        |"ts": {"cf": "myCf", "col": "timestamp", "type": "string"},
        |"ts2": {"cf": "myCf", "col": "timestamp2", "type": "string"},
        |"field": {"cf": "myCf", "col": "qualifier", "type": "long"},
        |"structField": {"cf": "myCf", "col": "q2", "avro": "avroSchema"}
      """.stripMargin.trim()
    )
  }

  it should "throw an exception if the same qualifier is define twice" in {
    intercept[IllegalStateException] {
      ColumnFamilyBuilder
        .withName("myCf")
        .withCellQualifier(KVColumn.primitive("field", "qualifier", KVType.LONG))
        .withCellQualifier(KVColumn.primitive("aaa", "qualifier", KVType.STRING))
        .withCellQualifier(KVColumn.avro("structField", "q2", "avroSchema"))
        .withClusteringColumn(KVColumn.primitive("ts", "timestamp", KVType.STRING))
        .withClusteringColumn(KVColumn.primitive("ts2", "timestamp2", KVType.STRING))
        .build
    }
  }

}
