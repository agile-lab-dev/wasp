package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.RawSparkBatchWriter
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models.RawModel
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{Column, DataFrame, Encoders, SparkSession}

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

class FolderCompaction extends Logging {
  import FolderCompactionUtils._
  /**
    * Receives the following conf:
    * {
    * "inputModel" : "name of the input model" | ModelConf,
    * "outputModel" : "name of the input model" | ModelConf,
    * "partitions" : {
    * "String1" : ["value1", "value2"],
    * "String2" : ["value1", "value2"]
    * },
    * "numPartitions" : "integer"
    * }
    *
    *
    * ModelConf has the following structure:
    *
    * {
    * "name": "a name you like"
    * "uri": "URI of the dataset: basePath if the dataset is partitioned"
    * "schema": "Spark json representation of the schema"
    * "timed": true/false
    * "options": {
    * saveMode: "spark save mode"
    * format: "spark data format"
    * extraOptions: {  // "extra format to the spark reader/writer"
    * key: value
    * }
    * partitionBy: [ // "partition columns"
    * partitionColumn1,
    * partitionColumn2,
    * ...
    * partitionColumnN
    * ]
    * }
    * }
    *
    * The function will retrieve or build the two indicated models, check that they are file-based,
    * and check that the indicated columns are partition columns. If the previous requirements are met then it
    * will generate each combination of column values and then perform read, repartition to the "numPartitions"
    * number, write to the outputModel and delete the files read.
    *
    * @param conf  the Config
    * @param spark the Spark session
    */
  def compact(conf: Config, spark: SparkSession): Unit = {

    val partitions: Map[String, List[String]] = parsePartitions(conf)
    val inputModel: RawModel = parseModel(conf, INPUT_MODEL_CONF_KEY)
    val outputModel: RawModel = parseModel(conf, OUTPUT_MODEL_CONF_KEY)
    val numPartitions = conf.getInt(NUM_PARTITIONS_CONF_KEY)

    compact(inputModel, outputModel, partitions, numPartitions, spark)
  }

  /**
    * See [[compact(conf:Config,spark:SparkSession)]]
    *
    * @param inputModel    the input model to read
    * @param outputModel   the output model to write
    * @param partitions    the partitions to compact and values that are part of the
    * @param numPartitions number of output partitions
    * @param spark         the Spark session
    */
  def compact(inputModel: RawModel,
              outputModel: RawModel,
              partitions: Map[String, List[String]],
              numPartitions: Int,
              spark: SparkSession): Unit = {
    val whereConditions = generateWhereConditions(partitions, inputModel, outputModel)
    val (dataframes, _) = read(spark, inputModel, partitions, whereConditions)

    val repartitionedDataFrames = dataframes.map(_.repartition(numPartitions))

    write(new RawSparkBatchWriter(outputModel, spark.sparkContext), repartitionedDataFrames)

    delete(spark, inputModel.uri, dataframes)
  }

  /** Deletes all files that have been read and all empty folders after
    *
    * @param spark   the Spark session
    * @param rootDir the root dir all dataframes originated from
    * @param dfs     Dataframes that have been read and filtered
    */
  def delete(spark: SparkSession,
             rootDir: String,
             dfs: List[DataFrame]): Unit = {
    dfs match {
      case Nil =>
        logger.info("Nothing to delete")
      case hDf :: tailDfs =>
        val unionDF = tailDfs.foldLeft(hDf)(_ union _)
        val files = unionDF.select(input_file_name()).distinct().as[String](Encoders.STRING).collect().toList

        files match {
          case Nil => logger.info("Nothing to delete")
          case head :: _ =>
            val fs = new Path(head).getFileSystem(spark.sparkContext.hadoopConfiguration)
            val pathsToDelete = files.map(s => fs.makeQualified(new Path(s)))

            logger.info("Deleting files:\n" + pathsToDelete.mkString("\t", "\n\t", ""))
            files.foreach(p => fs.delete(fs.makeQualified(new Path(p)), false))
            val deletedEmptyFolders = deleteEmptyPartitionFolders(fs, fs.makeQualified(new Path(rootDir)), pathsToDelete)
            logger.info(s"Deleted empty folders: ${deletedEmptyFolders.sortBy(_.toString).mkString(", ")}")
        }
    }
  }

  /**
    *
    * @param spark           the Spark session
    * @param inputModel      the input model to read
    * @param partitions      the partitions to compact and values that are part of the
    * @param whereConditions the where conditions to filter the Dataframe read from the inputModel
    * @return the list of DataFrames read, the list of files read from the inputModel
    */
  def read(spark: SparkSession,
           inputModel: RawModel,
           partitions: Map[String, List[String]],
           whereConditions: List[WhereCondition]): (List[DataFrame], List[Path]) = {
    logger.info(s"Initialize Spark HDFSReader with this model: $inputModel")

    // setup reader
    val trySchema = Try {
      DataType.fromJson(inputModel.schema).asInstanceOf[StructType]
    }
    val options = inputModel.options
    val reader = spark.read
      .format(options.format)
      .options(options.extraOptions.getOrElse(Map()))

    val readerWithSchema = trySchema match {
      case Failure(_) => reader
      case Success(schema) => reader.schema(schema)
    }

    val path = if (inputModel.timed) {
      // the path is timed; find and return the most recent subdirectory
      val hdfsPath = new Path(inputModel.uri)
      val hdfs = hdfsPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

      val subdirectories = hdfs.listStatus(hdfsPath)
        .toList
        .filter(_.isDirectory)
      val mostRecentSubdirectory = subdirectories.sortBy(_.getPath.getName)
        .reverse
        .head
        .getPath

      mostRecentSubdirectory.toString
    } else {
      // the path is not timed; return it as-is
      inputModel.uri
    }

    val hdfsPath = new Path(path)
    val hdfs = hdfsPath.getFileSystem(spark.sparkContext.hadoopConfiguration)

    // filter partitions declared in the inputModel
    val filteredPartitions = partitions.filter {
      case (colName, _) => inputModel.options.partitionBy.exists(_.contains(colName))
    }

    // retrieve all files belonging to the partitions specified in the input model
    val inputModelFiles: List[Path] = FolderCompactionUtils.discoverPartitionFiles(hdfs, hdfsPath)(filteredPartitions)

    logger.info(s"Loading from this path: '$path'")
    val unfilteredDF = readerWithSchema.load(path).unpersist(blocking = true)

    // filter out whereConditions not associated to any file from the input model
    val filteredWhereConditions: List[Column] = whereConditions.filter(filterWhereCondition(inputModelFiles))
      .map(_.toSparkColumn)
    logger.info(s"Where conditions: $filteredWhereConditions")

    // apply whereConditions to the read DataFrame
    val loadedDataFrames = filteredWhereConditions.map {
      query => unfilteredDF.where(query)
    }

    (loadedDataFrames, inputModelFiles)
  }

  /**
    *
    * @param fs            the filesystem to be used for deletion
    * @param dataframeRoot the root path of the dataset
    * @param deletedFiles  the files that have been just deleted from the fs
    * @return the paths that have been deleted
    */
  def deleteEmptyPartitionFolders(fs: FileSystem, dataframeRoot: Path, deletedFiles: List[Path]): List[Path] = {
    val foldersToDelete = deletedFiles.foldLeft(Set.empty[Path]) { (set, path) =>
      set + path.getParent
    }
    foldersToDelete.toList.flatMap { p =>
      deleteEmptyFoldersUpwards(fs, p, dataframeRoot)
    }
  }

  @tailrec
  final def isParent(child: Path, parentToFind: Path): Boolean = {
    child.getParent match {
      case null => false
      case parent =>
        parent == parentToFind || isParent(parent, parentToFind)
    }
  }


  private[tools] def deleteEmptyFoldersUpwards(fs: FileSystem, p: Path, until: Path): List[Path] = {
    @tailrec
    def rDeleteEmptyFoldersUpwards(fs: FileSystem, p: Path, until: Path, deletedSoFar: List[Path]): List[Path] = {
      if (p == until) {
        deletedSoFar
      } else {
        if (fs.listStatus(p).isEmpty) {
          fs.delete(p, false)
          rDeleteEmptyFoldersUpwards(fs, p.getParent, until, p :: deletedSoFar)
        } else {
          deletedSoFar
        }
      }
    }

    require(isParent(p, until), s"$p is not child of $until")
    rDeleteEmptyFoldersUpwards(fs, p, until, List.empty)
  }


  def write(writer: RawSparkBatchWriter,
            dataframes: List[DataFrame]): Unit = {
    dataframes.foreach {
      df => writer.write(df)
    }
  }

}
