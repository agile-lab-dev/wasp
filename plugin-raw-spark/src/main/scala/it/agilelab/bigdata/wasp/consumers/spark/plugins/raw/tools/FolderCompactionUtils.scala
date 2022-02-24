package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools

import com.typesafe.config.{Config, ConfigList, ConfigValueType}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools.WhereCondition.PartitionsCombination
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.models.{RawModel, RawOptions}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.JavaConverters._

object FolderCompactionUtils {

  val INPUT_MODEL_CONF_KEY = "inputModel"
  val OUTPUT_MODEL_CONF_KEY = "outputModel"
  val PARTITIONS_CONF_KEY = "partitions"
  val NUM_PARTITIONS_CONF_KEY = "numPartitions"

  type ColumnName = String
  type ColumnValue = String

  /**
    * Generates all the possible combinations of columnName and columnValue
    * Example:
    *   partitions = List(
    *     "a" -> List("1"),
    *     "b" -> List("2", "3"),
    *     "c" -> List("4", "5", "6")
    *   )
    *   output = List(
    *     ("a", "1") :: ("b", "2) :: ("c", "4") :: Nil,
    *     ("a", "1") :: ("b", "2") :: ("c", "5") :: Nil,
    *     ("a", "1") :: ("b", "2") :: ("c", "6") :: Nil,
    *     ("a", "1") :: ("b", "3") :: ("c", "4") :: Nil,
    *     ("a", "1") :: ("b", "3") :: ("c", "5") :: Nil,
    *     ("a", "1") :: ("b", "3") :: ("c", "6") :: Nil,
    *   )
    * @param partitions   the list of partitions to generate the combinations
    * @return             all the possible combinations obtained from the input partitions
    * */
  def generateCombinations(partitions: List[(ColumnName, List[ColumnValue])]): List[PartitionsCombination] = {
    partitions match {
      case Nil => Nil
      case (colName, colValues) :: tail =>
        generateCombinationsForValues(colName, colValues, tail)
    }
  }

  private def generateCombinationsForValues(col: ColumnName, values: List[ColumnValue],
                                          others: List[(ColumnName, List[ColumnValue])]): List[PartitionsCombination] = {
    values.flatMap(value => generateCombinationsForValue(col, value, others))
  }

  private def generateCombinationsForValue(col: String,
                                         value: String,
                                         others: List[(ColumnName, List[ColumnValue])]): List[PartitionsCombination] = {
    others match {
      case Nil =>
        List(List(Partition(col, value)))
      case (colName, colValues) :: tail =>
        generateCombinationsForValues(colName, colValues, tail).map(Partition(col, value) :: _)
    }
  }

  def parsePartitions(conf: Config): Map[ColumnName, List[ColumnValue]] = {
    if (!conf.hasPath(PARTITIONS_CONF_KEY)) {
      Map.empty[ColumnName, List[ColumnValue]]
    } else {
      val partitionsEntrySet = conf.getConfig(PARTITIONS_CONF_KEY).entrySet()
      partitionsEntrySet.asScala.map { entry =>
        entry.getValue match {
          case l: ConfigList =>
            entry.getKey -> l.unwrapped().asScala.toList.map(_.toString)
          case _ => throw new IllegalArgumentException("Wrong partition configuration format")
        }
      }.toMap
    }
  }

  def parseModel(conf: Config, key: String): RawModel = {
    conf.getValue(key).valueType() match {
      case ConfigValueType.OBJECT =>
        parseConfigModel(conf.getConfig(key))
      case ConfigValueType.STRING =>
        val str = conf.getString(key)
        ConfigBL.rawBL.getByName(str)
          .getOrElse(throw new IllegalArgumentException(s"Cannot find model with name $str"))
      case _ =>
        throw new IllegalArgumentException(s"Conf: ${conf.getValue(key)} is neither a String or " +
          s"a configuration")
    }
  }

  def parseConfigModel(conf: Config): RawModel = {
    val name = conf.getString("name")
    val uri = conf.getString("uri")
    val schema = conf.getString("schema")
    val timed = conf.getBoolean("timed")
    val options = {
      val saveMode = conf.getString("options.saveMode")
      val format = conf.getString("options.format")
      val partitionBy =
        if (conf.hasPath("options.partitionBy")) {
          Some(conf.getStringList("options.partitionBy").asScala.toList)
        } else {
          None
        }
      val extraOptions =
        if (conf.hasPath("options.extraOptions")) {
          Some(conf.getConfig("options.extraOptions").entrySet().asScala.map { entry =>
            entry.getKey -> entry.getValue.unwrapped().toString
          }.toMap)
        } else {
          None
        }
      RawOptions(saveMode = saveMode, format = format, extraOptions = extraOptions, partitionBy = partitionBy)
    }
    RawModel(name = name, uri = uri, timed = timed, schema = schema, options = options)
  }

  /**
    * Builds the list of WhereCondition used to filter the original DataFrame read
    * from the input model. This list has one element for each output partition combination,
    * in order to write the correct number of files to the partitions specified by the output model.
    * Each of these combinations is put in AND with all the input partitions combinations, in order
    * to write only the files of the partitions requested.
    * Example:
    *   inputModel.partitions = [a, b, c, d]
    *   outputModel.partitions = [a, b]
    *   partitions = a -> [1, 2], b -> [3, 4], c -> [5, 6], d -> [7, 8]
    *
    *   output = [
    *     (a=1 AND b=3) AND ( (c=5 AND d=7) OR (c=5 AND d=8) OR (c=6 AND d=7) OR (c=6 AND d=8) )
    *     (a=1 AND b=4) AND ( (c=5 AND d=7) OR (c=5 AND d=8) OR (c=6 AND d=7) OR (c=6 AND d=8) )
    *     (a=2 AND b=3) AND ( (c=5 AND d=7) OR (c=5 AND d=8) OR (c=6 AND d=7) OR (c=6 AND d=8) )
    *     (a=2 AND b=4) AND ( (c=5 AND d=7) OR (c=5 AND d=8) OR (c=6 AND d=7) OR (c=6 AND d=8) )
    *   ]
    * @param partitions   the list of partitions to generate the conditions
    * @param inputModel   the inputModel defining the input partitions
    * @param outputModel  the outputModel defining the output partitions
    * @return             the list of WhereCondition generated
    * */
  def generateWhereConditions(partitions: Map[ColumnName, List[ColumnValue]],
                              inputModel: RawModel,
                              outputModel: RawModel): List[WhereCondition] = {
    val inputPartitionColumns: Set[ColumnName] = inputModel.options.partitionBy
      .map(_.filter(partitions.contains).toSet)
      .getOrElse(Set.empty)
    val outputPartitionColumns: Set[ColumnName] = outputModel.options.partitionBy
      .map(_.filter(partitions.contains).toSet)
      .getOrElse(Set.empty)

    val commonColumns: Set[ColumnName] = inputPartitionColumns.intersect(outputPartitionColumns)
    val onlyInputColumns: Set[ColumnName] = inputPartitionColumns.diff(outputPartitionColumns)

    val outputPartitionsCombinations: List[PartitionsCombination] = generateCombinations(partitions.filter(x => commonColumns.contains(x._1)).toList)
    val inputOnlyPartitionsCombinations: List[PartitionsCombination] = generateCombinations(partitions.filter(x => onlyInputColumns.contains(x._1)).toList)

    val whereConditionsLists = outputPartitionsCombinations match {
      case Nil => List((Nil, inputOnlyPartitionsCombinations))
      case _ => outputPartitionsCombinations.map(_ -> inputOnlyPartitionsCombinations)
    }

    whereConditionsLists.map(x => WhereCondition(x._1, x._2))
  }

  /**
    * @param files            the list of files to
    * @param whereCondition   the WhereCondition to filter
    * @return                 true if at least one file is present for this WhereCondition, false otherwise
    */
  def filterWhereCondition(files: List[Path])
                          (whereCondition: WhereCondition): Boolean = {
    files.exists(filterSingleQuery(whereCondition))
  }

  /**
    * @param whereCondition   the WhereCondition to filter
    * @param file             the file to check
    * @return                 true if `whereCondition` covers the path of `file`, false otherwise
    */
  def filterSingleQuery(whereCondition: WhereCondition)
                       (file: Path): Boolean = {
    val folders = file.toString.split("/")

    val firstAnd = filterPartitionsCombination(folders, whereCondition.outPartitionsCombination)
    val secondAnd = whereCondition.inPartitionsCombinations match {
      case Nil => true
      case x => x.foldLeft(false) {
        case (acc, combination) => acc || filterPartitionsCombination(folders, combination)
      }
    }

    firstAnd && secondAnd
  }

  /**
    * @param folders      the list of directories that a file path contains
    *                       E.g. [ "journey", "raw", "a=1", "b=2" ]
    * @param combination  the combination of partitions
    *                       E.g. [ "a=1", "b=2" ]
    * @return             true if the partitions of `combinations` are all present in `folders`, false otherwise
    */
  def filterPartitionsCombination(folders: Array[String],
                                  combination: PartitionsCombination): Boolean = {
    combination.forall(partition => folders.contains(partition.toDirName))
  }

  /**
    * Given a basePath, finds all files associated to the specified partitions
    * @param fileSystem   the file system from which to read the files
    * @param basePath     the base path from which to begin the search
    * @param partitions   the partitions to search
    * @return             the list of Path of files found
    */
  def discoverPartitionFiles(fileSystem: FileSystem, basePath: Path)
                            (partitions: Map[ColumnName, List[ColumnValue]]): List[Path] = {
    val partitionsCombinations = FolderCompactionUtils.generateCombinations(partitions.toList).map {
      _.map(_.toDirName)
    }
    PartitionDiscoveryUtils.listFiles(fileSystem)(basePath)
      .filter { filePath =>
        partitionsCombinations.exists(filterPath(filePath))
      }
  }

  /**
    * @param path           the path of the file
    * @param combinations   the combination of partitions
    * @return               true if the partitions of `combinations` are all present in `path`, false otherwise
    */
  def filterPath(path: Path)
                (combinations: List[String]): Boolean = {
    val folders = path.toString.split("/")
    combinations.forall(folders.contains(_))
  }

}