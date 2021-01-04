package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools

import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.models.{RawModel, RawOptions}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import spray.json._


object FakeJsonFormat extends DefaultJsonProtocol {
  implicit val fakeJsonFormat: RootJsonFormat[FakeJson] = jsonFormat2(FakeJson)
  implicit val aFormat: RootJsonFormat[FakeJsonA] = jsonFormat3(FakeJsonA)
  implicit val bFormat: RootJsonFormat[FakeJsonB] = jsonFormat3(FakeJsonB)
  implicit val cFormat: RootJsonFormat[FakeJsonC] = jsonFormat3(FakeJsonC)
  implicit val abFormat: RootJsonFormat[FakeJsonAB] = jsonFormat4(FakeJsonAB)
  implicit val acFormat: RootJsonFormat[FakeJsonAC] = jsonFormat4(FakeJsonAC)
  implicit val bcFormat: RootJsonFormat[FakeJsonBC] = jsonFormat4(FakeJsonBC)
  implicit val abcFormat: RootJsonFormat[FakeJsonABC] = jsonFormat5(FakeJsonABC)
  implicit val fakeJsonEncoder: Encoder[FakeJson] = Encoders.product[FakeJson]
  implicit val aEncoder: Encoder[FakeJsonA] = Encoders.product[FakeJsonA]
  implicit val bEncoder: Encoder[FakeJsonB] = Encoders.product[FakeJsonB]
  implicit val cEncoder: Encoder[FakeJsonC] = Encoders.product[FakeJsonC]
  implicit val abEncoder: Encoder[FakeJsonAB] = Encoders.product[FakeJsonAB]
  implicit val acEncoder: Encoder[FakeJsonAC] = Encoders.product[FakeJsonAC]
  implicit val bcEncoder: Encoder[FakeJsonBC] = Encoders.product[FakeJsonBC]
  implicit val abcEncoder: Encoder[FakeJsonABC] = Encoders.product[FakeJsonABC]
}

class FolderCompactionSpec extends FlatSpec with Matchers with BeforeAndAfterEach with SparkSuite {

  import FolderCompactionSpec._
  import FakeJsonFormat._

  private lazy val fs: FileSystem = FileSystem.getLocal(new Configuration())
  private lazy val testFolder = fs.makeQualified(new Path("./inputFolder"))
  private lazy val outFolder = fs.makeQualified(new Path("./outFolder"))
  private val schema = new StructType()
    .add("uri", StringType)
    .add("hash", IntegerType)

  private lazy val target = new FolderCompaction

  val test1FilesToCreate = List(
    (p: Path) => p / "a=b" / "b=c" / "c=d" / "0.json",
    (p: Path) => p / "a=b" / "b=c" / "c=d" / "1.json",
    (p: Path) => p / "a=b" / "b=c" / "c=f" / "2.json",
    (p: Path) => p / "a=b" / "b=c" / "c=g" / "3.json",
    (p: Path) => p / "a=b" / "b=c" / "c=h" / "4.json",
    (p: Path) => p / "a=x" / "b=c" / "c=g" / "5.json",
    (p: Path) => p / "a=x" / "b=d" / "c=h" / "6.json"
  )

  val test2FilesToCreate = List(
    (p: Path) => p / "a.parquet",
    (p: Path) => p / "b.parquet",
    (p: Path) => p / "c.parquet"
  )

  val test3FilesToCreate = List(
    (p: Path) => p / "a=b" / "b=c" / "xyz.parquet",
    (p: Path) => p / "a=b" / "b=c" / "c=f" / "xyz.parquet",
    (p: Path) => p / "a=b" / "xyz.parquet"
  )

  val test4FilesToCreate = List(
    (p: Path) => p / "a=b" / "b=c" / "xyz.parquet",
    (p: Path) => p / "b=b" / "b=c" / "xyz.parquet",
    (p: Path) => p / "b=a" / "b=c" / "xyz.parquet"
  )


  it should "correctly compact 1" in {
    val inPartitions = List("a", "b", "c")
    val outPartitions = List("a")
    val partitions = Map(
      "a" -> List("b", "emptyFolder"),
      "b" -> List("c"),
      "c" -> List("d", "f", "g", "h")
    )

    val inputOptions = RawOptions(saveMode = "append", format = "json", extraOptions = None, partitionBy = Some(inPartitions))
    val outputOptions = inputOptions.copy(partitionBy = Some(outPartitions))
    val inputModel = RawModel(name = "inModel", uri = "./inputFolder", timed = false, schema = "{}", options = inputOptions)
    val outputModel = RawModel(name = "outModel", uri = "./outFolder", timed = false, schema = "{}", options = outputOptions)

    val reader = spark.read
      .format("json")
      .schema(schema)
    val readerBC = reader.schema(schema.add("b", StringType).add("c", StringType))

    val originalData = reader.load("./inputFolder").as[FakeJsonBC]
      .where(col("a") === lit("b"))
      .where(col("b") === lit("c"))
      .where(col("c").isin("d", "f", "g", "h")).collect()

    val target = new FolderCompaction
    target.compact(inputModel, outputModel, partitions, 1, spark)

    // all files in a=b/b=c should be compacted
    fs.exists(new Path("./inputFolder/a=b/b=c")) shouldBe false
    // a=x/* should not be compacted
    reader.load("./inputFolder/a=x").as[FakeJson].collect().length shouldBe 2

    // empty `partition` a=emptyFolder should be filtered
    val outFolders = PartitionDiscoveryUtils.listDirectories(fs)(outFolder)
    outFolders.length shouldBe 1
    outFolders.head.getName shouldBe "a=b"

    // there should be only one file compacted in a=b
    val outFiles = PartitionDiscoveryUtils.listFiles(fs)(outFolder).filterNot(_.getName == "_SUCCESS")
    outFiles.length shouldBe 1
    outFiles.head.getParent shouldBe outFolder.suffix("/a=b")

    // the compacted file should contain all the values from the files compacted
    readerBC.load("./outFolder").as[FakeJsonBC].collect() should contain theSameElementsAs originalData
  }

  it should "correctly compact 2" in {
    val inPartitions = List("a", "b", "c")
    val outPartitions = List("c")
    val partitions = Map(
      "a" -> List("b"),
      "b" -> List("c"),
      "c" -> List("d", "f", "g")
    )

    val inputOptions = RawOptions(saveMode = "append", format = "json", extraOptions = None, partitionBy = Some(inPartitions))
    val outputOptions = inputOptions.copy(partitionBy = Some(outPartitions))
    val inputModel = RawModel(name = "inModel", uri = "./inputFolder", timed = false, schema = "{}", options = inputOptions)
    val outputModel = RawModel(name = "outModel", uri = "./outFolder", timed = false, schema = "{}", options = outputOptions)

    val reader = spark.read
      .format("json")
      .schema(schema)
    val readerAB = reader.schema(schema.add("a", StringType).add("b", StringType))

    val originalData = reader.load("./inputFolder").as[FakeJsonAB]
      .where(col("a") === lit("b"))
      .where(col("b") === lit("c"))
      .where(col("c").isin("d", "f", "g")).collect()

    val target = new FolderCompaction
    target.compact(inputModel, outputModel, partitions, 1, spark)

    // NOT all files in a=b/b=c should be compacted
    reader.load("./inputFolder/a=b/b=c").rdd.isEmpty() shouldBe false
    // a=b/b=c/c=h should not be compacted
    reader.load("./inputFolder/a=b/b=c").as[FakeJson].collect() should contain theSameElementsAs {
      Array(FakeJson("4.json", "4.json".hashCode))
    }
    // a=x/* should not be compacted
    reader.load("./inputFolder/a=x").as[FakeJson].collect().length shouldBe 2

    // the compacted file should contain all the values from the files compacted
    readerAB.load("./outFolder").as[FakeJsonAB].collect() should contain theSameElementsAs originalData
  }

  it should "correctly compact 3" in {
    val inPartitions = List("a", "b", "c")
    val outPartitions = List()
    val partitions = Map(
      "a" -> List("b"),
      "b" -> List("c"),
      "c" -> List("d", "f", "g", "h")
    )

    val inputOptions = RawOptions(saveMode = "append", format = "json", extraOptions = None, partitionBy = Some(inPartitions))
    val outputOptions = inputOptions.copy(partitionBy = Some(outPartitions))
    val inputModel = RawModel(name = "inModel", uri = "./inputFolder", timed = false, schema = "{}", options = inputOptions)
    val outputModel = RawModel(name = "outModel", uri = "./outFolder", timed = false, schema = "{}", options = outputOptions)

    val reader = spark.read
      .format("json")
      .schema(schema)
    val readerBC = reader.schema(schema.add("b", StringType).add("c", StringType))

    val originalData = reader.load("./inputFolder").as[FakeJsonBC]
      .where(col("a") === lit("b"))
      .where(col("b") === lit("c"))
      .where(col("c").isin("d", "f", "g", "h")).collect()

    target.compact(inputModel, outputModel, partitions, 1, spark)

    // all files in a=b/b=c should be compacted
    fs.exists(new Path("./inputFolder/a=b/b=c")) shouldBe false
    // a=x/* should not be compacted
    reader.load("./inputFolder/a=x").as[FakeJson].collect().length shouldBe 2

    // there should be only one file without partitioning
    val outFiles = PartitionDiscoveryUtils.listFiles(fs)(outFolder).filterNot(_.getName == "_SUCCESS")
    outFiles.length shouldBe 1
    outFiles.head.getParent shouldBe outFolder

    // the compacted file should contain all the values from the files compacted
    readerBC.load("./outFolder").as[FakeJsonBC].collect() should contain theSameElementsAs originalData
  }

  it should "do not delete any folder if no files have been deleted" in {
    val testFiles = test1FilesToCreate.map(_.apply(testFolder))
    target.deleteEmptyPartitionFolders(fs, testFolder, testFiles)
    testFiles.foreach { p =>
      fs.exists(p) shouldBe true
    }
  }

  it should "not delete any folder if they are still not empty" in {
    val testFiles = test1FilesToCreate.map(_.apply(testFolder))
    // delete  / "a=b" / "b=c" / "c=d" / "0.json",
    testFiles.take(1).foreach(p => fs.delete(p, false))
    target.deleteEmptyPartitionFolders(fs, testFolder, testFiles.take(1))
    testFiles.foreach { p =>
      fs.exists(p.getParent) shouldBe true
    }
  }

  it should "delete only the empty folder" in {
    val testFiles = test1FilesToCreate.map(_.apply(testFolder))
    // delete
    //  / "a=b" / "b=c" / "c=d" / "0.json",
    //  / "a=b" / "b=c" / "c=d" / "1.json",
    testFiles.take(2).foreach(p => fs.delete(p, false))
    target.deleteEmptyPartitionFolders(fs, testFolder, testFiles.take(2))
    testFiles.take(2).foreach { p =>
      fs.exists(p.getParent) shouldBe false
    }
    testFiles.drop(2).foreach { p =>
      fs.exists(p) shouldBe true
    }
  }

  it should "delete only the empty folder 2" in {
    val testFiles = test1FilesToCreate.map(_.apply(testFolder))
    // delete
    // / "a=x" / "b=c" / "c=g" / "5.json",
    // / "a=x" / "b=d" / "c=h" / "6.json"
    testFiles.takeRight(2).foreach(p => fs.delete(p, false))
    target.deleteEmptyPartitionFolders(fs, testFolder, testFiles.takeRight(2))
    testFiles.takeRight(2).foreach { p =>
      fs.exists(p.getParent.getParent.getParent) shouldBe false // a=x
    }
    testFiles.dropRight(2).foreach { p =>
      fs.exists(p) shouldBe true
    }
  }

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    createFiles(fs, test1FilesToCreate.map(_ (testFolder)))
  }

  override protected def afterEach(): Unit = {
    fs.delete(testFolder, true)
    fs.delete(outFolder, true)
    super.afterEach()
  }

  def createFiles(fs: FileSystem,
                  paths: List[Path]): Unit = {
    for (p <- paths) {
      fs.mkdirs(p.getParent)
      val file: FSDataOutputStream = fs.create(p, true)
      file.writeBytes(FakeJson(p.getName, p.getName.hashCode()).toJson.toString())
      file.close()
    }
  }
}

object FolderCompactionSpec {

  implicit class RichPath(val p: Path) extends AnyVal {
    def /(suffix: String): Path = {
      require(!suffix.contains("/"))
      p.suffix("/" + suffix)
    }
  }

}


case class FakeJson(uri: String, hash: Int)

case class FakeJsonA(uri: String, hash: Int, a: String)

case class FakeJsonB(uri: String, hash: Int, b: String)

case class FakeJsonC(uri: String, hash: Int, c: String)

case class FakeJsonAB(uri: String, hash: Int, a: String, b: String)

case class FakeJsonAC(uri: String, hash: Int, a: String, c: String)

case class FakeJsonBC(uri: String, hash: Int, b: String, c: String)

case class FakeJsonABC(uri: String, hash: Int, a: String, b: String, c: String)