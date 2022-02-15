package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import org.scalatest.{Args, BeforeAndAfterEach, Status, Suite}

import java.io.File
import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.collection.JavaConverters._

trait TempDirectoryTest extends BeforeAndAfterEach { this: Suite =>
  protected def tempDir: String

  lazy val path = new File(tempDir).toPath

  override def beforeEach(): Unit = {
    super.beforeEach()
    Files.createDirectories(path)
  }

  override def afterEach(): Unit = {
    try {
      Files.walk(path)
        .sorted(Comparator.reverseOrder[Path]())
        .iterator()
        .asScala
        .map(_.toFile)
        .foreach(_.delete())

    } finally {
      super.afterEach()
    }

  }
  override abstract def runTest(testName: String, args: Args): Status = super[BeforeAndAfterEach].runTest(testName, args)
}