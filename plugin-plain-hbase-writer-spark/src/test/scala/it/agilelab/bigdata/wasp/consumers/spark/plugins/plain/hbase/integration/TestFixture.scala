package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, WordSpec}

import java.io.File

class TestFixture extends WordSpec with Matchers with BeforeAndAfter {

  def sparkSession: SparkSession = SparkSession
    .builder
    .master("local[2]")
    .appName("hbase-test")
    .getOrCreate()

  val checkpointLocation = "/tmp/checkpoint"

  before {
    try {
      FileUtils.deleteDirectory(new File(checkpointLocation))
    } catch {
      case _: Exception => //do nothing
    }
  }

}
