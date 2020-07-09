package it.agilelab.bigdata.wasp.models.builder

import it.agilelab.bigdata.wasp.models.builder.KVColumnFamily.ColumnFamilyBuilder
import org.scalatest.{FlatSpec, Matchers}

class KVSchemaBuilderTest extends FlatSpec with Matchers {

  it should "be impossible to build an incomplete KVSchemaBuilder" in {
    assertTypeError("""KVSchemaBuilder.emptySchemaBuilder.build""")
    assertTypeError("""KVSchemaBuilder.withKey("myKey").build""")
  }

  it should "throw an exception if the same column family is defined twice" in {
    intercept[IllegalStateException] {
      KVSchemaBuilder.emptySchemaBuilder
        .withKey("rowkey" -> KVType.STRING)
        .withFamily(
          ColumnFamilyBuilder
            .withName("m")
            .withCellQualifier(KVColumn.primitive((x: BigClass) => x.aString, "cq1"))
            .build)
        .withFamily(
          ColumnFamilyBuilder
            .withName("m")
            .withCellQualifier(KVColumn.primitive((x: BigClass) => x.aString, "cq2"))
            .build).build
    }
  }

}
