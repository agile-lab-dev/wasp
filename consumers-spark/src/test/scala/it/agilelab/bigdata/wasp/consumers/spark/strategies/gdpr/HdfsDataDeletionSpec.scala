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
import org.scalatest._

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
  val correlationIdColumn: String = "correlationId"

  val target = new HdfsDataDeletion(fs)
  implicit val dataEncoder: Encoder[Data] = Encoders.product[Data]
  implicit val dataWithTupleEncoder: Encoder[DataWithKey] = Encoders.product[DataWithKey]
  implicit val dataWithDateEncoder: Encoder[DataWithDate] = Encoders.product[DataWithDate]
  implicit val dataWithDateNumericEncoder: Encoder[DataWithDateNumeric] = Encoders.product[DataWithDateNumeric]
  implicit val keyWithCorrelationEncoder: Encoder[KeyWithCorrelation] = Encoders.product[KeyWithCorrelation]

  it should "correctly delete data from flat RawModel" in {
    val data: Seq[Data] = Seq(
      Data("k1", "111L", "franco"),
      Data("k2", "222L", "mario"),
      Data("k3", "333L", "luigi")
    )
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k4", "id3")
    )
    val expectedData: Seq[Data] = data.filterNot { d =>
      keysToDelete.exists(k => k.key == d.id)
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
      correlationIdColumn,
      rawModel,
      ExactRawMatchingStrategy(keyColumn),
      NoPartitionPruningStrategy()
    )

    val config = createConfig(None)
    val deletionConfig = HdfsDeletionConfig.create(config, rawDataStoreConf, keysToDelete)

    val deletionResult = target.delete(deletionConfig, spark)


    readData[Data] should contain theSameElementsAs expectedData
    deletionResult.get should contain theSameElementsAs
      keysToDelete.map { k =>
        val keyExists = data.exists(d => d.id == k.key)
        DeletionOutput(
          k,
          HdfsExactColumnMatch(keyColumn),
          if (keyExists) HdfsParquetSource(fileNames.filter { case (_, key) => k.key == key }.map(_._1)) else NoSourceFound,
          if (keyExists) DeletionSuccess else DeletionNotFound
        )
      }
  }

  it should "correctly delete data from flat RawModel matching with an expression" in {
    val data: Seq[DataWithKey] = Seq(
      DataWithKey(KeyWithCorrelation("k1", "1"), "111L", "franco"),
      DataWithKey(KeyWithCorrelation("k2", "1"), "222L", "mario"),
      DataWithKey(KeyWithCorrelation("k3", "1"), "333L", "luigi")
    )
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k4", "id3")
    )
    val expectedData: Seq[DataWithKey] = data.filterNot { d =>
      keysToDelete.exists(k => k.key == d.id.key)
    }

    writeTestData(data, 3, List.empty)

    val keyColumn = nameOf[DataWithKey](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[DataWithKey].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, None)
    )

    import spark.implicits._
    val fileNames = readFileNameAndExpr("id.key")

    val rawDataStoreConf = RawDataStoreConf(
      keyColumn,
      correlationIdColumn,
      rawModel,
      ExactRawMatchingStrategy("id.key"),
      NoPartitionPruningStrategy()
    )

    val config = createConfig(None)
    val deletionConfig = HdfsDeletionConfig.create(config, rawDataStoreConf, keysToDelete)

    val deletionResult = target.delete(deletionConfig, spark)

    val expectedDeletions = keysToDelete.map { k =>
      val keyExists = data.exists(d => d.id.key == k.key)
      DeletionOutput(
        k,
        HdfsExactColumnMatch("id.key"),
        if (keyExists) HdfsParquetSource(fileNames.filter { case (_, key) => k.key == key }.map(_._1)) else NoSourceFound,
        if (keyExists) DeletionSuccess else DeletionNotFound
      )
    }

    readData[DataWithKey] should contain theSameElementsAs expectedData
    deletionResult.get should contain theSameElementsAs expectedDeletions
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
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k3", "id3")
    )
    val expectedData: Seq[Data] = data.filterNot { d =>
      keysToDelete.exists(k => k.key == d.id)
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
      correlationIdColumn,
      rawModel,
      ExactRawMatchingStrategy(keyColumn),
      NoPartitionPruningStrategy()
    )

    val config = createConfig(None)
    val deletionConfig = HdfsDeletionConfig.create(config, rawDataStoreConf, keysToDelete)

    val deletionResult = target.delete(deletionConfig, spark)

    readData[Data] should contain theSameElementsAs expectedData
    deletionResult.get should contain theSameElementsAs
      keysToDelete.map { k =>
        val keyExists = data.exists(d => d.id == k.key)
        DeletionOutput(
          k,
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
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k3", "id3")
    )
    val expectedData: Seq[DataWithDate] = data.filterNot(d => d.id == "k2" || d.id == "k3suffix" || d.id == "k3suffix2")

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
      correlationIdColumn,
      rawModel,
      PrefixRawMatchingStrategy(keyColumn),
      TimeBasedBetweenPartitionPruningStrategy(nameOf[DataWithDate](_.date), isDateNumeric = false, "yyyyMMddHHmm")
    )

    val config = createConfig(Some(1570658400000L, 1571090400000L))
    val deletionConfig = HdfsDeletionConfig.create(config, rawDataStoreConf, keysToDelete)

    val deletionResult = target.delete(deletionConfig, spark)

    readData[DataWithDate] should contain theSameElementsAs expectedData
    val expectedDeletions = Seq(
      DeletionOutput(
        "k1",
        HdfsPrefixColumnMatch(
          keyColumn,
          None),
        NoSourceFound,
        DeletionNotFound,
        "id1"
      ),
      DeletionOutput(
        "k2",
        HdfsPrefixColumnMatch(
          keyColumn,
          Some(Seq("k2"))),
        HdfsParquetSource(fileNames.filter { case (_, key) => key == "k2" }.map(_._1)),
        DeletionSuccess,
        "id2"
      ),
      DeletionOutput(
        "k3",
        HdfsPrefixColumnMatch(
          keyColumn,
          Some(Seq("k3suffix", "k3suffix2"))),
        HdfsParquetSource(fileNames.filter { case (_, key) => key == "k3suffix" || key == "k3suffix2" }.map(_._1)),
        DeletionSuccess,
        "id3"
      )
    )

    deletionResult.get.foreach { toCompare =>
      val expected = expectedDeletions.find(d => d.key == toCompare.key).get
      compareOutput(toCompare, expected)
    }
  }

  it should "correctly delete data using time strategy with a numeric date" in {
    val data: Seq[DataWithDateNumeric] = Seq(
      DataWithDateNumeric("k1", "111", 1570258400000L, "aaa"), // Sat Oct 05 2019 06:53:20
      DataWithDateNumeric("k2", "222", 1570758400001L, "bbb"), // Fri Oct 11 2019 01:46:40
      DataWithDateNumeric("k3", "333", 1570258400000L, "ccc"), // Sat Oct 05 2019 06:53:20
      DataWithDateNumeric("k3suffix", "111", 1570258400000L, "ddd"), // Sat Oct 05 2019 06:53:20
      DataWithDateNumeric("k5", "222", 1570258400000L, "eee"), // Sat Oct 05 2019 06:53:20
      DataWithDateNumeric("k6", "333", 1570258400000L, "fff") // Sat Oct 05 2019 06:53:20
    )
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k3", "id3")
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
      correlationIdColumn,
      rawModel,
      ExactRawMatchingStrategy(keyColumn),
      TimeBasedBetweenPartitionPruningStrategy(nameOf[DataWithDateNumeric](_.date), isDateNumeric = true, "yyyyMMddHHmm")
    )

    // 201910100000 - 201910150000
    val config = createConfig(Some((1570658400000L, 1571090400000L)))
    val deletionConfig = HdfsDeletionConfig.create(config, rawDataStoreConf, keysToDelete)

    val deletionResult = target.delete(deletionConfig, spark)

    val expectedDeletions = Seq(
      DeletionOutput(
        "k1",
        HdfsExactColumnMatch(keyColumn),
        NoSourceFound,
        DeletionNotFound,
        "id1"
      ),
      DeletionOutput(
        "k2",
        HdfsExactColumnMatch(keyColumn),
        HdfsParquetSource(fileNames.filter { case (_, key) => key == "k2" }.map(_._1)),
        DeletionSuccess,
        "id2"
      ),
      DeletionOutput(
        "k3",
        HdfsExactColumnMatch(keyColumn),
        NoSourceFound,
        DeletionNotFound,
        "id3"
      )
    )

    readData[DataWithDateNumeric] should contain theSameElementsAs expectedData
    deletionResult.get.foreach { toCompare =>
      val expected = expectedDeletions.find(d => d.key == toCompare.key).get
      compareOutput(toCompare, expected)
    }
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
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k3", "id3")
    )
    val expectedData: Seq[Data] = data.filterNot { d =>
      keysToDelete.exists(k => k.key == d.id)
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
      correlationIdColumn,
      rawModel,
      ExactRawMatchingStrategy(keyColumn),
      NoPartitionPruningStrategy()
    )

    val deletionConfig = HdfsDeletionConfig.create(createConfig(None), rawDataStoreConf, keysToDelete)

    val deletionResult = target.delete(deletionConfig, spark)

    readData[Data] should contain theSameElementsAs expectedData
    deletionResult.get should contain theSameElementsAs
      keysToDelete.map { k =>
        val keyExists = data.exists(d => d.id == k.key)
        DeletionOutput(
          k,
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
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k3", "id3")
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

    val rawDataStoreConf = RawDataStoreConf(
      keyColumn,
      correlationIdColumn,
      rawModel,
      PrefixRawMatchingStrategy(keyColumn),
      NoPartitionPruningStrategy()
    )

    val expectedDeletions = Seq(
      DeletionOutput(
        "k1",
        HdfsPrefixColumnMatch(keyColumn, Some(Seq("k1|asd"))),
        HdfsParquetSource(fileNames.filter { case (_, key) => key.startsWith("k1") }.map(_._1)),
        DeletionSuccess,
        "id1"
      ),
      DeletionOutput(
        "k2",
        HdfsPrefixColumnMatch(keyColumn, Some(Seq("k2|asd"))),
        HdfsParquetSource(fileNames.filter { case (_, key) => key.startsWith("k2") }.map(_._1)),
        DeletionSuccess,
        "id2"
      ),
      DeletionOutput(
        "k3",
        HdfsPrefixColumnMatch(keyColumn, Some(Seq("k3"))),
        HdfsParquetSource(fileNames.filter { case (_, key) => key.startsWith("k3") }.map(_._1)),
        DeletionSuccess,
        "id3"
      )
    )

    val deletionConfig = HdfsDeletionConfig.create(createConfig(None), rawDataStoreConf, keysToDelete)

    val deletionResult = target.delete(deletionConfig, spark)

    readData[Data] should contain theSameElementsAs expectedData
    deletionResult.get.foreach { toCompare =>
      val expected = expectedDeletions.find(d => d.key == toCompare.key).get
      compareOutput(toCompare, expected)
    }
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
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k3", "id3")
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
      keysToDelete, rawModel, ExactRawMatchingStrategy(keyColumn), ALWAYS_TRUE_COLUMN, ALWAYS_TRUE_COLUMN, stagingUri, backupUri
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
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k3", "id3")
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
      keysToDelete, rawModel, ExactRawMatchingStrategy(keyColumn), ALWAYS_TRUE_COLUMN, ALWAYS_TRUE_COLUMN, stagingUri, backupUri
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
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k3", "id3")
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
      keysToDelete, rawModel, ExactRawMatchingStrategy(keyColumn), ALWAYS_TRUE_COLUMN, ALWAYS_TRUE_COLUMN, stagingUri, backupUri
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
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k3", "id3")
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
      keysToDelete, rawModel, ExactRawMatchingStrategy(keyColumn), ALWAYS_TRUE_COLUMN, ALWAYS_TRUE_COLUMN, stagingUri, backupUri
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
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k3", "id3")
    )
    val expectedData: Seq[Data] = data.filterNot { d =>
      keysToDelete.exists(k => k.key == d.id)
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
      correlationIdColumn,
      rawModel,
      ExactRawMatchingStrategy(keyColumn),
      NoPartitionPruningStrategy()
    )
    val deletionConfig = HdfsDeletionConfig.create(createConfig(None), rawDataStoreConf, keysToDelete)

    val deletionHandler = new HdfsDeletionHandler(fs, deletionConfig, spark)
    val backupHandler = new HdfsBackupHandler(fs, dataPath.getParent, dataPath) {
      override def deleteBackup(backupPath: Path): Try[Unit] = Failure(new Exception(""))
    }

    target.delete(deletionHandler, backupHandler, deletionConfig, dataPath, spark).failure.exception shouldBe a[BackupDeletionException]

    // keys should have been deleted
    readData[Data] should contain theSameElementsAs expectedData
  }

  it should "correctly fail when a path is missing and missingPathFailure=true" in {
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k4", "id3")
    )

    val keyColumn = nameOf[Data](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, None)
    )

    val rawDataStoreConf = RawDataStoreConf(
      keyColumn,
      correlationIdColumn,
      rawModel,
      ExactRawMatchingStrategy(keyColumn),
      NoPartitionPruningStrategy(),
      missingPathFailure = true
    )

    val config = createConfig(None)
    val deletionConfig = HdfsDeletionConfig.create(config, rawDataStoreConf, keysToDelete)

    val deletionResult = target.delete(deletionConfig, spark)

    deletionResult.failure.exception shouldBe a[DeletionException]
    deletionResult.failure.exception.getMessage should startWith(s"Path does not exist: file:$dataUri")
  }

  it should "return a NOT_FOUND when a path is missing and missingPathFailure=false" in {
    val keysToDelete: Seq[KeyWithCorrelation] = Seq(
      KeyWithCorrelation("k1", "id1"),
      KeyWithCorrelation("k2", "id2"),
      KeyWithCorrelation("k4", "id3")
    )

    val keyColumn = nameOf[Data](_.id)

    val rawModel = RawModel(
      "data",
      dataUri,
      timed = false,
      ScalaReflection.schemaFor[Data].dataType.asInstanceOf[StructType].json,
      RawOptions("append", "parquet", None, None)
    )

    val rawDataStoreConf = RawDataStoreConf(
      keyColumn,
      correlationIdColumn,
      rawModel,
      ExactRawMatchingStrategy(keyColumn),
      NoPartitionPruningStrategy(),
      missingPathFailure = false
    )

    val config = createConfig(None)
    val deletionConfig = HdfsDeletionConfig.create(config, rawDataStoreConf, keysToDelete)

    val deletionResult = target.delete(deletionConfig, spark)

    deletionResult.get should contain theSameElementsAs
      keysToDelete.map { k =>
        DeletionOutput(
          k,
          HdfsExactColumnMatch(keyColumn),
          NoSourceFound,
           DeletionNotFound
        )
      }
  }



  private def createConfig(startAndEnd: Option[(Long, Long)]): Config = {
    val string =
      s""" { "$RAW_CONF_KEY" { """ +
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

  def readFileNameAndExpr(expression: String)(implicit ev: Encoder[(String, String)]): Seq[(FileName, KeyName)] = {
    spark.read
      .format("parquet")
      .load(dataUri)
      .select(input_file_name(), expr(expression))
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

  private def compareOutput(toCompare: DeletionOutput, expected: DeletionOutput): Assertion = {
    toCompare.key shouldBe expected.key
    expected.keyMatchType match {
      case hdfs: HdfsMatchType => hdfs match {
        case HdfsExactColumnMatch(columnName) => toCompare.keyMatchType.asInstanceOf[HdfsExactColumnMatch].columnName shouldBe columnName
        case HdfsPrefixColumnMatch(columnName, Some(matchedValues)) =>
          toCompare.keyMatchType.asInstanceOf[HdfsPrefixColumnMatch].columnName shouldBe columnName
          toCompare.keyMatchType.asInstanceOf[HdfsPrefixColumnMatch].matchedValues.get should contain theSameElementsAs matchedValues
        case HdfsPrefixColumnMatch(columnName, None) =>
          toCompare.keyMatchType.asInstanceOf[HdfsPrefixColumnMatch].columnName shouldBe columnName
          toCompare.keyMatchType.asInstanceOf[HdfsPrefixColumnMatch].matchedValues shouldBe None
      }
      case _ => fail("unexpected HBaseMatchType")
    }
    expected.source match {
      case HdfsParquetSource(fileNames) => toCompare.source.asInstanceOf[HdfsParquetSource].fileNames should contain theSameElementsAs fileNames
      case NoSourceFound => toCompare.source shouldBe NoSourceFound
      case _ => fail("unexpected HBaseTableSource")
    }
    toCompare.result shouldBe expected.result
    toCompare.correlationId shouldBe expected.correlationId
  }

}

case class Data(id: String, category: String, name: String)

case class DataWithKey(id: KeyWithCorrelation, category: String, name: String)

case class DataWithDate(id: String, category: String, date: String, name: String)

case class DataWithDateNumeric(id: String, category: String, date: Long, name: String)
