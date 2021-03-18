package it.agilelab.bigdata.wasp.consumers.spark.plugins.cdc

import java.net.URL
import java.nio.file.{Path, Paths}

import org.apache.commons.io.FileUtils
import org.scalatest.{BeforeAndAfterEach, Suite}


trait TempDirectoryEach extends BeforeAndAfterEach { self: Suite =>
  private var _tempDir: Path = _

  val tmpFolder = System.getProperty("java.io.tmpdir")
  protected def tempDir: String = "file://" + tmpFolder + "/_temp_dir"

  override def beforeEach(): Unit = {
    super.beforeEach()
    _tempDir = Paths.get(new URL(tempDir).toURI)
  }

  override def afterEach(): Unit =
    try {
      FileUtils.deleteDirectory(_tempDir.toFile)
    } finally {
      super.afterEach()
    }
}
