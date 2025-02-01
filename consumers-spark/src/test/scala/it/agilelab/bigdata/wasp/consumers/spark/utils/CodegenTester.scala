package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.apache.spark.sql.internal.SQLConf

trait CodegenTester extends SparkSuite {

  def testAllCodegen(f: => Unit) = {
    testWholestageCodegen(f)
    testNonWholestageCodegen(f)
  }

  def testWholestageCodegen(f: => Unit) = {
    val defValue = spark.conf.getOption(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key).getOrElse("true")
    spark.sql(s"set ${SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key}=true")
    spark.sql(s"set ${SQLConf.CODEGEN_FALLBACK.key}=false")
    f
    spark.sql(s"set ${SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key}=$defValue")
  }

  def testNonWholestageCodegen(f: => Unit) = {
    val defValue = spark.conf.getOption(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key).getOrElse("true")
    spark.sql(s"set ${SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key}=false")
    spark.sql(s"set ${SQLConf.CODEGEN_FALLBACK.key}=false")
    f
    spark.sql(s"set ${SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key}=$defValue")
  }

}
