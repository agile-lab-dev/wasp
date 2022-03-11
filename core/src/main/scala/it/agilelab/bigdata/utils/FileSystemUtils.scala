package it.agilelab.bigdata.utils

import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.utils.Utils

import java.nio.file.{ Files, Path }
import java.util.Comparator
import scala.collection.JavaConverters._

object FileSystemUtils extends Logging {

  def recursivelyDeleteDirectory(path: Path): Unit =
    try {
      if (Files.exists(path)) {
        Utils.using(
          Files
            .walk(path)
        )(
          _.sorted(Comparator.reverseOrder[Path]())
            .iterator()
            .asScala
            .map(_.toFile)
            .foreach(_.delete())
        )
      }
    } catch {
      case e: Exception => logger.warn(s"Couldn't delete files in $path", e)
    }

}
