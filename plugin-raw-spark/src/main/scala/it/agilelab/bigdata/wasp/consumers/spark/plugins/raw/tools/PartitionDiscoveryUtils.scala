package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools

import org.apache.hadoop.fs.{FileSystem, Path}

import scala.annotation.tailrec

object PartitionDiscoveryUtils {

  /** Recursively lists all the files in the input directory **/
  def listFiles(fs: FileSystem)
               (path: Path): List[Path] = {
    @tailrec
    def rListFiles(paths: List[Path], soFar: List[Path]): List[Path] = {
      paths match {
        case Nil =>
          soFar
        case head :: tail =>
          val (files, directories) = fs.listStatus(head).toList.partition(_.isFile)
          rListFiles(tail ::: directories.map(_.getPath), soFar ::: files.map(_.getPath))
      }
    }

    rListFiles(List(path), Nil)
  }

  /** Recursively lists all the subdirectories in the input directory **/
  def listDirectories(fs: FileSystem)
                     (path: Path): List[Path] = {
    @tailrec
    def rListDirectories(paths: List[Path], soFar: List[Path]): List[Path] = {
      paths match {
        case Nil =>
          soFar
        case head :: tail =>
          val (_, directories) = fs.listStatus(head).toList.partition(_.isFile)
          rListDirectories(tail ::: directories.map(_.getPath), soFar ::: directories.map(_.getPath))
      }
    }

    rListDirectories(List(path), Nil)
  }


  /**
    * Calculates the depth of the currentPath relative ot basePath
    */
  @tailrec
  final def fileDepth(fs: FileSystem)
                     (basePath: Path, currentPath: Path, currDepth: Int = 0): Int = {
    require(fs.makeQualified(currentPath).toString.contains(fs.makeQualified(basePath).toString),
      s"$basePath has to be an ancestor of $currentPath"
    )
    if (currentPath == basePath) {
      currDepth
    } else {
      fileDepth(fs)(basePath, currentPath.getParent, currDepth + 1)
    }
  }

  /**
    * Goes up to the passed number of level with regard to input p
    */
  @tailrec
  private final def goUp(p: Path, depth: Int): Path = {
    require(depth >= 0)
    if (depth == 0) {
      p
    } else {
      goUp(p.getParent, depth - 1)
    }
  }

  /**
    * Discovers all the partitions columns and values of a given basePath
    *
    * @return a Left with an error message if something goes wrong during the partition discovery.
    *         a Right with a list of PartitionColumnValues. The list is ordered from the closest to the basePath
    *         column to the farthest. PartitionColumnValues are sorted alphabetically.
    */
  def discoverPartitions(fs: FileSystem)
                        (basePath: Path): Either[String, List[PartitionColumnValues]] = {
    val files = listFiles(fs)(basePath).map(_.getParent).distinct
    Either.cond(files.nonEmpty, Nil, s"No files found under $basePath").right.flatMap { _ =>
      val depths = files.map(fileDepth(fs)(basePath, _))
      val depth = depths.head
      Either.cond(depth >= 0 && depths.forall(_ == depth), depth - 1, s"Depth of all files should be the same")
    }.right.flatMap { depth =>
      val columnAndValues = (0 to depth).toList.reverse.map { d =>
        columnAndValuesAtDepth(files, d)
      }
      Either.cond(columnAndValues.forall(_.isRight), columnAndValues.map(_.right.get), columnAndValues.find(_.isLeft).get.left.get)
    }.right.flatMap(x =>
      Either.cond(x.forall(y => y.forall(_._1 == y.head._1)), x, s"Column names at the same depth should be the same")
    ).right.map(
      _.map(l => PartitionColumnValues(l.head._1, l.map(_._2)))
    )
  }

  /**
    * Discovers the column name and value of a given path at a given depth.
    *
    * @return a Left with an error message if something goes wrong during the value discovery.
    *         a Right with a list of column name and column value, this does not check that the layout is correct just
    *         that all the sub-folders have a compatible "name" (i.e. in the form of col=value)
    */
  def columnAndValuesAtDepth(files: List[Path], depth: Int): Either[String, List[(String, String)]] = {
    val columnsAndValues = files.map(goUp(_, depth)).distinct.map(getColumnAndValue)
    Either.cond(
      columnsAndValues.forall(_.isRight),
      columnsAndValues.map(_.right.get),
      columnsAndValues.find(_.isLeft).get.left.get)
  }

  /**
    * Given a path returns column name and value of the leaf of that path, if any present enclosed in a Right.
    * If any error occurs, the returned value will be a Left enclosing an error description
    */
  def getColumnAndValue(p: Path): Either[String, (String, String)] = {
    val splits = p.getName.split("=", 2)
    if (splits.length == 2) {
      Right(splits(0) -> splits(1))
    } else {
      Left(s"${p.getName} is not a partition column")
    }

  }
}
