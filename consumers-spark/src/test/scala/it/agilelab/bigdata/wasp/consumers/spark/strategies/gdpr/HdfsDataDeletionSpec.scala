package it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr

import java.io.IOException
import java.nio.file.Paths

import com.github.dwickern.macros.NameOf._
import com.typesafe.config.{Config, ConfigFactory}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.config.HdfsDeletionConfig
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.config.HdfsDeletionConfig._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.exception._
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hdfs.HdfsDataDeletion.{FileName, KeyName}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.hdfs.{HdfsBackupHandler, HdfsDataDeletion, HdfsDeletionHandler}
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.core.models._
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Encoder, Encoders}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers, TryValues}

import scala.util.{Failure, Try}

class HdfsDataDeletionSpec extends FlatSpec with Matchers with TryValues with BeforeAndAfterEach with SparkSuite with Logging {

  val tmpUri: String = System.getProperty("java.io.tmpdir") + "/wasp"
  val tmpPath: Path = new Path(tmpUri)

  val fs: FileSystem = FileSystem.newInstance(spark.sparkContext.hadoopConfiguration)
  val testResourcesPath = new Path(Paths.get(".").toAbsolutePath
    .resolve("consumers-spark")
    .resolve("src")
    .resolve("test")
    .resolve("resources")
    .resolve("gdpr").toAbsolutePath.toUri.toString)

  val dataUri: String = tmpUri + "/gdpr/data"
  val dataPath: Path = new Path(dataUri)
  val inputUri: String = tmpUri + "/gdpr/input"
  val stagingUri: String = tmpUri + "/gdpr/staging"
  val backupUri: String = tmpUri + "/gdpr/backup"

  val target = new HdfsDataDeletion(fs)
  implicit val dataEncoder: Encoder[Data] = Encoders.product[Data]
  implicit val dataWithDateEncoder: Encoder[DataWithDate] = Encoders.product[DataWithDate]
  implicit val dataWithDateNumericEncoder: Encoder[DataWithDateNumeric] = Encoders.product[DataWithDateNumeric]

  it should "correctly delete data from flat RawModel" in {
    val data: Seq[Data] = Seq(
      Data("k1", "111L", "franco"),
      Data("k2", "222L", "mario"),
      Data("k3", "333L", "luigi")
    )
    val keysToDelete: Seq[Key] = Seq(
      Key("k1"),
      Key("k2"),
      Key("k4")
    )
    val expectedData: Seq[Data] = data.filterNot { d =>
      keysToDelete.contains(Key(d.id))
    }

    writeTestData(data, 3, List.empty)

    val keyColumn = nameOf[Data](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, None)
    )

    import spark.implicits._
    val fileNames = readFileNameAndId

    val rawDataStoreConf = RawDataStoreConf(
      keyColumn,
      rawModel,
      ExactRawMatchingStrategy(keyColumn),
      NoPartitionPruningStrategy()
    )

    val config = createConfig(None)
    val deletionConfig = HdfsDeletionConfig.create(config, rawDataStoreConf, keysToDelete.map(_.key))

    val deletionResult = target.delete(deletionConfig, spark)


    readData[Data] should contain theSameElementsAs expectedData
    deletionResult.get should contain theSameElementsAs
      keysToDelete.map { k =>
        val keyExists = data.exists(d => d.id == k.key)
        DeletionOutput(
          k.key,
          HdfsExactColumnMatch(keyColumn),
          if (keyExists) HdfsParquetSource(fileNames.filter { case (_, key) => k.key == key }.map(_._1)) else NoSourceFound,
          if (keyExists) DeletionSuccess else DeletionNotFound
        )
      }
  }

  it should "correctly delete data from partitioned RawModel" in {
    val data: Seq[Data] = Seq(
      Data("k1", "111", "aaa"),
      Data("k2", "222", "bbb"),
      Data("k3", "333", "ccc"),
      Data("k3suffix", "111", "ddd"),
      Data("k5", "222", "eee"),
      Data("k6", "333", "fff")
    )
    val keysToDelete: Seq[Key] = Seq(
      Key("k1"),
      Key("k2"),
      Key("k3")
    )
    val expectedData: Seq[Data] = data.filterNot { d =>
      keysToDelete.contains(Key(d.id))
    }
    val partitionBy = List("category")

    writeTestData(data, 1, partitionBy)

    val keyColumn = nameOf[Data](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, Some(partitionBy))
    )

    import spark.implicits._
    val fileNames = readFileNameAndId

    val rawDataStoreConf = RawDataStoreConf(
      keyColumn,
      rawModel,
      ExactRawMatchingStrategy(keyColumn),
      NoPartitionPruningStrategy()
    )

    val config = createConfig(None)
    val deletionConfig = HdfsDeletionConfig.create(config, rawDataStoreConf, keysToDelete.map(_.key))

    val deletionResult = target.delete(deletionConfig, spark)

    readData[Data] should contain theSameElementsAs expectedData
    deletionResult.get should contain theSameElementsAs
      keysToDelete.map { k =>
        val keyExists = data.exists(d => d.id == k.key)
        DeletionOutput(
          k.key,
          HdfsExactColumnMatch(keyColumn),
          if (keyExists) HdfsParquetSource(fileNames.filter { case (_, key) => k.key == key }.map(_._1)) else NoSourceFound,
          if (keyExists) DeletionSuccess else DeletionNotFound
        )
      }
  }

  it should "correctly delete data using time strategy with a non numeric date" in {
    val data: Seq[DataWithDate] = Seq(
      DataWithDate("k1", "111", "201910201140", "aaa"),
      DataWithDate("k2", "222", "201910131543", "bbb"),
      DataWithDate("k3", "333", "201910201140", "ccc"),
      DataWithDate("k3suffix", "111", "201910131140", "ddd"),
      DataWithDate("k3suffix2", "222", "201910131140", "ddd"),
      DataWithDate("k5", "222", "201910201140", "eee"),
      DataWithDate("k6", "333", "201910121140", "fff")
    )
    val keysToDelete: Seq[Key] = Seq(
      Key("k1"),
      Key("k2"),
      Key("k3")
    )
    val expectedData: Seq[DataWithDate] = data.filterNot(d => d.id == "k2" || d.id == "k3suffix" || d.id == "k3suffix2")
    val expectedDataDeleted: Seq[String] = Seq("k2", "k3suffix", "k3suffix2")

    val partitionBy = List("category")

    writeTestData(data, 1, partitionBy)

    val keyColumn = nameOf[DataWithDate](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[DataWithDate].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, Some(partitionBy))
    )

    import spark.implicits._
    val fileNames = readFileNameAndId

    val rawDataStoreConf = RawDataStoreConf(
      keyColumn,
      rawModel,
      PrefixRawMatchingStrategy(keyColumn),
      TimeBasedBetweenPartitionPruningStrategy(nameOf[DataWithDate](_.date), isDateNumeric = false, "yyyyMMddHHmm")
    )

    val config = createConfig(Some(1570658400000L, 1571090400000L))
    val deletionConfig = HdfsDeletionConfig.create(config, rawDataStoreConf, keysToDelete.map(_.key))

    val deletionResult = target.delete(deletionConfig, spark)

    readData[DataWithDate] should contain theSameElementsAs expectedData
/*
    val toMatch = Seq(
      DeletionOutput(
        "k1",
        HdfsPrefixColumnMatch(
          keyColumn,
          None),
        NoSourceFound,
        DeletionNotFound
      ),
      DeletionOutput(
        "k2",
        HdfsPrefixColumnMatch(
          keyColumn,
          Some(Seq("k2"))),
        HdfsParquetSource(fileNames.filter { case (_, key) => key == "k2" }.map(_._1)),
        DeletionSuccess
      ),
      DeletionOutput(
        "k3",
        HdfsPrefixColumnMatch(
          keyColumn,
          Some(Seq("k3suffix", "k3suffix2"))),
        HdfsParquetSource(fileNames.filter { case (_, key) => key == "k3suffix" || key == "k3suffix2" }.map(_._1)),
        DeletionSuccess
      )
    )

    deletionResult.get should contain theSameElementsAs
     Seq(
       DeletionOutput(
         "k1",
         HdfsPrefixColumnMatch(
           keyColumn,
           None),
         NoSourceFound,
         DeletionNotFound
       ),
       DeletionOutput(
         "k2",
         HdfsPrefixColumnMatch(
           keyColumn,
           Some(Seq("k2"))),
         HdfsParquetSource(fileNames.filter { case (_, key) => key == "k2" }.map(_._1)),
         DeletionSuccess
       ),
       DeletionOutput(
         "k3",
         HdfsPrefixColumnMatch(
           keyColumn,
           Some(Seq("k3suffix", "k3suffix2"))),
         HdfsParquetSource(fileNames.filter { case (_, key) => key == "k3suffix" || key == "k3suffix2" }.map(_._1)),
         DeletionSuccess
       )
     )*/
  }

  it should "correctly delete data using time strategy with a numeric date" in {
    val data: Seq[DataWithDateNumeric] = Seq(
      DataWithDateNumeric("k1", "111", 1570258400000L, "aaa"),
      DataWithDateNumeric("k2", "222", 1570758400001L, "bbb"),
      DataWithDateNumeric("k3", "333", 1570258400000L, "ccc"),
      DataWithDateNumeric("k3suffix", "111", 1570258400000L, "ddd"),
      DataWithDateNumeric("k5", "222", 1570258400000L, "eee"),
      DataWithDateNumeric("k6", "333", 1570258400000L, "fff")
    )
    val keysToDelete: Seq[Key] = Seq(
      Key("k1"),
      Key("k2"),
      Key("k3")
    )
    val expectedData: Seq[DataWithDateNumeric] = data.filterNot(_.id == "k2")
    val partitionBy = List("category")

    writeTestData(data, 1, partitionBy)

    val keyColumn = nameOf[DataWithDateNumeric](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[DataWithDateNumeric].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, Some(partitionBy))
    )

    import spark.implicits._
    val fileNames = readFileNameAndId

    val rawDataStoreConf = RawDataStoreConf(
      keyColumn,
      rawModel,
      ExactRawMatchingStrategy(keyColumn),
      TimeBasedBetweenPartitionPruningStrategy(nameOf[DataWithDateNumeric](_.date), isDateNumeric = true, "yyyyMMddHHmm")
    )

    // 201910100000 - 201910150000
    val config = createConfig(Some((1570658400000L, 1571090400000L)))
    val deletionConfig = HdfsDeletionConfig.create(config, rawDataStoreConf, keysToDelete.map(_.key))

    val deletionResult = target.delete(deletionConfig, spark)

    readData[DataWithDateNumeric] should contain theSameElementsAs expectedData
    /*    deletionResult.get should contain theSameElementsAs
        data.map { d =>
          val keyExists = d.id == "k2" || d.id == "k3suffix" || d.id == "k3suffix2"
          DeletionOutput(
            k.key,
            HdfsExactColumnMatch(keyColumn),
            if (keyExists) HdfsParquetSource(fileNames.find { case (_, key) => k.key == key }.get._1) else NoSourceFound,
            if (keyExists) DeletionSuccess else DeletionNotFound
          )
        }
          keysToDelete.map { k =>
            val keyExists = k.key == "k2"
            DeletionOutput(
              k.key,
              HdfsExactColumnMatch(keyColumn),
              if (keyExists) HdfsParquetSource(fileNames.find { case (_, key) => k.key == key }.get._1) else NoSourceFound,
              if (keyExists) DeletionSuccess else DeletionNotFound
            )
          }*/
  }

  it should "should work when uris contain trailing slashes" in {
    val data: Seq[Data] = Seq(
      Data("k1", "111", "aaa"),
      Data("k2", "222", "bbb"),
      Data("k3", "333", "ccc"),
      Data("k3suffix", "111", "ddd"),
      Data("k5", "222", "eee"),
      Data("k6", "333", "fff")
    )
    val keysToDelete: Seq[Key] = Seq(
      Key("k1"),
      Key("k2"),
      Key("k3")
    )
    val expectedData: Seq[Data] = data.filterNot { d =>
      keysToDelete.contains(Key(d.id))
    }
    val partitionBy = List("category")

    writeTestData(data, 1, partitionBy)

    val keyColumn = nameOf[Data](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, Some(partitionBy))
    )

    import spark.implicits._
    val fileNames = readFileNameAndId

    val rawDataStoreConf = RawDataStoreConf(
      keyColumn,
      rawModel,
      ExactRawMatchingStrategy(keyColumn),
      NoPartitionPruningStrategy()
    )

    val deletionConfig = HdfsDeletionConfig.create(createConfig(None), rawDataStoreConf, keysToDelete.map(_.key))

    val deletionResult = target.delete(deletionConfig, spark)

    readData[Data] should contain theSameElementsAs expectedData
    deletionResult.get should contain theSameElementsAs
      keysToDelete.map { k =>
        val keyExists = data.exists(d => d.id == k.key)
        DeletionOutput(
          k.key,
          HdfsExactColumnMatch(keyColumn),
          if (keyExists) HdfsParquetSource(fileNames.filter { case (_, key) => k.key == key }.map(_._1)) else NoSourceFound,
          if (keyExists) DeletionSuccess else DeletionNotFound
        )
      }
  }


  it should "correctly delete data from partitioned RawModel with prefix strategy" in {
    val data: Seq[Data] = Seq(
      Data("k1|asd", "111", "aaa"),
      Data("k2|asd", "222", "bbb"),
      Data("k3", "333", "ccc"),
      Data("k4", "111", "ddd"),
      Data("k5", "222", "eee"),
      Data("k6", "333", "fff")
    )
    val keysToDelete: Seq[Key] = Seq(
      Key("k1"),
      Key("k2"),
      Key("k3")
    )
    val expectedData: Seq[Data] = data.filterNot { d =>
      d.id == "k1|asd" || d.id == "k2|asd" || d.id == "k3"
    }
    val partitionBy = List("category")

    writeTestData(data, 1, partitionBy)

    val keyColumn = nameOf[Data](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, Some(partitionBy))
    )

    import spark.implicits._
    val fileNames = readFileNameAndId
    println(fileNames)
    /*val expectedKeysDeleted =
      keysToDelete.map { k =>
        val keyExists = data.exists(d => d.id.startsWith(k.key))
        DeletionOutput(
          k.key,
          HdfsPrefixColumnMatch(keyColumn, (data.map(_.id).find(_.startsWith(k.key))),
          if (keyExists) HdfsParquetSource(fileNames.find { case (_, keyFromFile) => keyFromFile.startsWith(k.key) }.get._1) else NoSourceFound,
          if (keyExists) DeletionSuccess else DeletionNotFound
        )
      }*/

    val rawDataStoreConf = RawDataStoreConf(
      keyColumn,
      rawModel,
      PrefixRawMatchingStrategy(keyColumn),
      NoPartitionPruningStrategy()
    )

    val deletionConfig = HdfsDeletionConfig.create(createConfig(None), rawDataStoreConf, keysToDelete.map(_.key))

    val deletionResult = target.delete(deletionConfig, spark).get

    readData[Data] should contain theSameElementsAs expectedData
//    deletionResult should contain theSameElementsAs expectedKeysDeleted
  }

  it should "fail during backup" in {
    val data: Seq[Data] = Seq(
      Data("k1", "111", "aaa"),
      Data("k2", "222", "bbb"),
      Data("k3", "333", "ccc"),
      Data("k4", "111", "ddd"),
      Data("k5", "222", "eee"),
      Data("k6", "333", "fff")
    )
    val keysToDelete: Seq[Key] = Seq(
      Key("k1"),
      Key("k2"),
      Key("k3")
    )
    val partitionBy = List("category")

    writeTestData(data, 1, partitionBy)

    val keyColumn = nameOf[Data](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, Some(partitionBy))
    )

    val config = HdfsDeletionConfig(
      keysToDelete.map(_.key), rawModel, ExactRawMatchingStrategy(keyColumn), ALWAYS_TRUE_COLUMN, ALWAYS_TRUE_COLUMN, stagingUri, backupUri
    )

    val backupHandler = new HdfsBackupHandler(fs, dataPath.getParent, dataPath) {
      override def backup(filesToBackup: Seq[Path]): Try[Path] = Failure(new Exception(""))
    }
    val deletionHandler = new HdfsDeletionHandler(fs, config, spark)

    target.delete(deletionHandler, backupHandler, config, dataPath, spark).failure.exception shouldBe a[BackupException]

    // no key should have been deleted
    readData[Data] should contain theSameElementsAs data
  }

  it should "fail during deletion and restore backup" in {
    val data: Seq[Data] = Seq(
      Data("k1", "111", "aaa"),
      Data("k2", "222", "bbb"),
      Data("k3", "333", "ccc"),
      Data("k4", "111", "ddd"),
      Data("k5", "222", "eee"),
      Data("k6", "333", "fff")
    )
    val keysToDelete: Seq[Key] = Seq(
      Key("k1"),
      Key("k2"),
      Key("k3")
    )
    val partitionBy = List("category")

    writeTestData(data, 1, partitionBy)

    val keyColumn = nameOf[Data](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, Some(partitionBy))
    )

    val config = HdfsDeletionConfig(
      keysToDelete.map(_.key), rawModel, ExactRawMatchingStrategy(keyColumn), ALWAYS_TRUE_COLUMN, ALWAYS_TRUE_COLUMN, stagingUri, backupUri
    )

    val deletionHandler = new HdfsDeletionHandler(fs, config, spark) {
      override def delete(filesToFilter: List[String]): Try[Unit] = Failure(new IOException("fs exception"))
    }
    val backupHandler = new HdfsBackupHandler(fs, dataPath.getParent, dataPath)

    target.delete(deletionHandler, backupHandler, config, dataPath, spark).failure.exception shouldBe a[DeletionException]

    // no key should have been deleted
    readData[Data] should contain theSameElementsAs data
  }

  it should "restore backup stored in another directory" in {
    val data: Seq[Data] = Seq(
      Data("k1", "111", "aaa"),
      Data("k2", "222", "bbb"),
      Data("k3", "333", "ccc"),
      Data("k4", "111", "ddd"),
      Data("k5", "222", "eee"),
      Data("k6", "333", "fff")
    )
    val keysToDelete: Seq[Key] = Seq(
      Key("k1"),
      Key("k2"),
      Key("k3")
    )
    val partitionBy = List("category")

    writeTestData(data, 1, partitionBy)

    val keyColumn = nameOf[Data](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, Some(partitionBy))
    )

    val config = HdfsDeletionConfig(
      keysToDelete.map(_.key), rawModel, ExactRawMatchingStrategy(keyColumn), ALWAYS_TRUE_COLUMN, ALWAYS_TRUE_COLUMN, stagingUri, backupUri
    )

    val deletionHandler = new HdfsDeletionHandler(fs, config, spark) {
      override def delete(filesToFilter: List[String]): Try[Unit] = Failure(new IOException("fs exception"))
    }
    val backupHandler = new HdfsBackupHandler(fs, new Path("/tmp"), dataPath)

    target.delete(deletionHandler, backupHandler, config, dataPath, spark).failure.exception shouldBe a[DeletionException]

    // no key should have been deleted
    readData[Data] should contain theSameElementsAs data
  }


  it should "fail during backup restoration after deletion failure" in {
    val data: Seq[Data] = Seq(
      Data("k1", "111", "aaa"),
      Data("k2", "222", "bbb"),
      Data("k3", "333", "ccc"),
      Data("k4", "111", "ddd"),
      Data("k5", "222", "eee"),
      Data("k6", "333", "fff")
    )
    val keysToDelete: Seq[Key] = Seq(
      Key("k1"),
      Key("k2"),
      Key("k3")
    )
    val partitionBy = List("category")

    writeTestData(data, 1, partitionBy)

    val keyColumn = nameOf[Data](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, Some(partitionBy))
    )

    val config = HdfsDeletionConfig(
      keysToDelete.map(_.key), rawModel, ExactRawMatchingStrategy(keyColumn), ALWAYS_TRUE_COLUMN, ALWAYS_TRUE_COLUMN, stagingUri, backupUri
    )

    val deletionHandler = new HdfsDeletionHandler(new MockFileSystem(), config, spark) {
      override def delete(filesToFilter: List[String]): Try[Unit] = Failure(new IOException("fs exception"))
    }
    val backupHandler = new HdfsBackupHandler(fs, dataPath.getParent, dataPath) {
      override def restoreBackup(backupPath: Path): Try[Unit] = Failure(new Exception(""))
    }

    target.delete(deletionHandler, backupHandler, config, dataPath, spark).failure.exception shouldBe a[FailureDuringBackupRestorationException]

    // no key should have been deleted
    readData[Data] should contain theSameElementsAs data
  }

  it should "fail during backup directory removal after successful deletion" in {
    val data: Seq[Data] = Seq(
      Data("k1", "111", "aaa"),
      Data("k2", "222", "bbb"),
      Data("k3", "333", "ccc"),
      Data("k4", "111", "ddd"),
      Data("k5", "222", "eee"),
      Data("k6", "333", "fff")
    )
    val keysToDelete: Seq[Key] = Seq(
      Key("k1"),
      Key("k2"),
      Key("k3")
    )
    val expectedData: Seq[Data] = data.filterNot { d =>
      keysToDelete.contains(Key(d.id))
    }
    val partitionBy = List("category")

    writeTestData(data, 1, partitionBy)

    val keyColumn = nameOf[Data](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, Some(partitionBy))
    )
    val rawDataStoreConf = RawDataStoreConf(
      keyColumn,
      rawModel,
      ExactRawMatchingStrategy(keyColumn),
      NoPartitionPruningStrategy()
    )
    val deletionConfig = HdfsDeletionConfig.create(createConfig(None), rawDataStoreConf, keysToDelete.map(_.key))

    val deletionHandler = new HdfsDeletionHandler(fs, deletionConfig, spark)
    val backupHandler = new HdfsBackupHandler(fs, dataPath.getParent, dataPath) {
      override def deleteBackup(backupPath: Path): Try[Unit] = Failure(new Exception(""))
    }

    target.delete(deletionHandler, backupHandler, deletionConfig, dataPath, spark).failure.exception shouldBe a[BackupDeletionException]

    // keys should have been deleted
    readData[Data] should contain theSameElementsAs expectedData
  }

  private def createConfig(startAndEnd: Option[(Long, Long)]): Config = {
    val string = s""" { "$RAW_CONF_KEY" { """ +
      startAndEnd.fold("") { case (start, end) => s""" "$START_PERIOD_KEY" = $start, "$END_PERIOD_KEY" = $end, """ } +
      s""" "backupDir" = $backupUri, "stagingDir" = $stagingUri } } """
    ConfigFactory.parseString(string)
  }

  private def writeTestData[T](data: Seq[T], partitions: Int, partitionBy: List[String])(implicit encoder: Encoder[T]): Unit = {
    try {
      fs.delete(tmpPath, true)
    } catch {
      case _: Throwable => ()
    }
    writeData(data, partitions, partitionBy)
  }

  override protected def afterEach(): Unit = {
    super.afterEach()
    fs.delete(tmpPath, true)
  }

  def readData[T](implicit encoder: Encoder[T]): Seq[T] = {
    //    implicit val encoder: Encoder[T] = Encoders.product[T]
    spark.read
      .format("parquet")
      .load(dataUri)
      .as[T]
      .collect()
      .toSeq
  }

  def readFileNameAndId(implicit ev: Encoder[(String, String)]): Seq[(FileName, KeyName)] = {
    spark.read
      .format("parquet")
      .load(dataUri)
      .select(input_file_name(), col(nameOf[Data](_.id)))
      .as[(String, String)]
      .collect()
      .toSeq
  }

  def writeData[T](data: Seq[T], partitions: Int, partitionBy: List[String])(implicit encoder: Encoder[T]): Unit = {
    val ds = spark.createDataset(data)
      .repartition(partitions)
    ds.write
      .partitionBy(partitionBy: _*)
      .parquet(dataUri)
  }

}

case class Key(key: String)

case class Data(id: String, category: String, name: String)

case class DataWithDate(id: String, category: String, date: String, name: String)

case class DataWithDateNumeric(id: String, category: String, date: Long, name: String)
