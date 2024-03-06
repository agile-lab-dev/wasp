package it.agilelab.bigdata.wasp.consumers.spark.utils

import it.agilelab.bigdata.wasp.core.utils.SparkTestKit
import org.apache.spark.sql.internal.SQLConf

trait CodegenTester extends SparkTestKit {

  def testAllCodegen(f: => Unit) = {
    testWholestageCodegen(f)
    testNonWholestageCodegen(f)
  }

  def testWholestageCodegen(f: => Unit) = {
    val defValue = ss.conf.getOption(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key).getOrElse("true")
    ss.sql(s"set ${SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key}=true")
    ss.sql(s"set ${SQLConf.CODEGEN_FALLBACK.key}=false")
    f
    ss.sql(s"set ${SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key}=$defValue")
  }

  def testNonWholestageCodegen(f: => Unit) = {
    val defValue = ss.conf.getOption(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key).getOrElse("true")
    ss.sql(s"set ${SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key}=false")
    ss.sql(s"set ${SQLConf.CODEGEN_FALLBACK.key}=false")
    f
    ss.sql(s"set ${SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key}=$defValue")
  }

}
