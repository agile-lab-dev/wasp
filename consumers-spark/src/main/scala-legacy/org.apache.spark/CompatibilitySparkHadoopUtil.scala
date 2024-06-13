package org.apache.spark

import org.apache.spark.deploy.SparkHadoopUtil


/**
 * Contains util methods to interact with Hadoop from Spark.
 */

object CompatibilitySparkHadoopUtil {

  def get = SparkHadoopUtil.get

}
