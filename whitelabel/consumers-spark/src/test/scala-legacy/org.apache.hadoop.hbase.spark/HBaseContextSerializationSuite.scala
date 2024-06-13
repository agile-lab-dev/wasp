package org.apache.hadoop.hbase.spark

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class HBaseContextSerializationSuite extends FunSuite {
  test("hbase context is serializable") {
    val spark = SparkSession.builder().master("local[4]").getOrCreate()
    try {
      SparkEnv.get.closureSerializer
        .newInstance()
        .serialize(new HBaseContext(spark.sparkContext, spark.sparkContext.hadoopConfiguration, null))
    } catch {
      case e: Throwable => fail("Serialization should not fail!",e)
    }
  }
}
