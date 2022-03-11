package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import it.agilelab.bigdata.utils.FileSystemUtils
import org.scalatest.{ Args, BeforeAndAfterEach, Status, Suite }

import java.nio.file.Files

trait TempDirectoryTest extends BeforeAndAfterEach { this: Suite =>

  private lazy val tmpDirFile = {
    val className = this.getClass.getName.split('.').last
    Files.createTempDirectory(s"TmpDirOfClass$className")
  }

  lazy val tempDir: String = tmpDirFile.toUri.toString

  override def afterEach(): Unit = FileSystemUtils.recursivelyDeleteDirectory(tmpDirFile)

  override abstract def runTest(testName: String, args: Args): Status =
    super[BeforeAndAfterEach].runTest(testName, args)
}
