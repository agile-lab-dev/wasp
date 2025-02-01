package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfter, Matchers, WordSpec}

import java.io.File

class TestFixture extends WordSpec with Matchers with BeforeAndAfter {

  val checkpointLocation = "/tmp/checkpoint"

  before {
    try {
      FileUtils.deleteDirectory(new File(checkpointLocation))
    } catch {
      case _: Exception => //do nothing
    }
  }

}
