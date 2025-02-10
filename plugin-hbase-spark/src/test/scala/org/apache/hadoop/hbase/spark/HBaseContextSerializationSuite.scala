package org.apache.hadoop.hbase.spark

import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import org.apache.spark.SparkEnv
import org.scalatest.{FunSuite, Matchers}

class HBaseContextSerializationSuite extends FunSuite with Matchers with SparkSuite {
  test("hbase context is serializable") {
    noException shouldBe thrownBy {
      val _ = spark // do not remove, this initializes the context
      SparkEnv.get.closureSerializer
        .newInstance()
        .serialize(new HBaseContext(spark.sparkContext, spark.sparkContext.hadoopConfiguration, null))
    }
  }
}
