package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools.PartitionDiscoveryUtilsSpec._

class PartitionDiscoveryUtilsSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  private lazy val fs: FileSystem = FileSystem.getLocal(new Configuration())
  private lazy val test1Folder = fs.makeQualified(new Path("./partitionedTable"))
  private lazy val test2Folder = fs.makeQualified(new Path("./notPartitionedTable"))
  private lazy val test3Folder = fs.makeQualified(new Path("./strangeLayout"))
  private lazy val test4Folder = fs.makeQualified(new Path("./wrongColumnNames"))

  val test1FilesToCreate = List(
    (p: Path) => p / "a=b" / "b=c" / "c=d" / "xyz.parquet",
    (p: Path) => p / "a=b" / "b=c" / "c=f" / "xyz.parquet",
    (p: Path) => p / "a=b" / "b=c" / "c=g" / "xyz.parquet",
    (p: Path) => p / "a=b" / "b=c" / "c=h" / "xyz.parquet"
  )

  val test2FilesToCreate = List(
    (p: Path) => p / "xyz.parquet",
    (p: Path) => p / "xyz.parquet",
    (p: Path) => p / "xyz.parquet"
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


  it should "list all the files in a given directory" in {
    //    createFiles(fs, test1FilesToCreate.map(_ (test1Folder)))
    fs.mkdirs(test1Folder / "a=b" / "b=c" / "c=p")
    PartitionDiscoveryUtils.listFiles(fs)(test1Folder) should contain theSameElementsAs test1FilesToCreate.map(_ (test1Folder))
  }

  it should "calculate depth 0 when passing the same folder twice" in {
    PartitionDiscoveryUtils.fileDepth(fs)(test1Folder, test1Folder) should be(0)
  }

  it should "calculate depth correctly when invoking with a partitioned table" in {
    PartitionDiscoveryUtils.fileDepth(fs)(test1Folder, test1Folder / "a=b" / "b=c" / "c=h" / "xyz.parquet") should be(4)
  }

  it should "find the partitions where are some" in {
    val partitions = PartitionDiscoveryUtils.discoverPartitions(fs)(test1Folder).right.get

    partitions.head.name shouldBe "a"
    partitions.head.values should contain theSameElementsAs List("b")
    partitions(1).name shouldBe "b"
    partitions(1).values should contain theSameElementsAs List("c")
    partitions(2).name shouldBe "c"
    partitions(2).values should contain theSameElementsAs List("d", "f", "g", "h")
  }

  it should "find no partitions where there are none" in {
    val result = PartitionDiscoveryUtils.discoverPartitions(fs)(test2Folder)
    result.isRight should be(true)
    result.right.get shouldBe empty
  }

  it should "return an error when the folder layout is not correct" in {
    val result = PartitionDiscoveryUtils.discoverPartitions(fs)(test3Folder)
    result.isLeft should be(true)
    result.left.get should be("Depth of all files should be the same")
  }

  it should "return an error when the column names are mixed" in {
    val result = PartitionDiscoveryUtils.discoverPartitions(fs)(test4Folder)
    result.isLeft should be(true)
    result.left.get should be("Column names at the same depth should be the same")
  }

  def createFiles(fs: FileSystem, paths: List[Path]): Unit = {
    for (p <- paths) {
      fs.mkdirs(p.getParent)
      fs.create(p, true).close()
    }
  }


  override protected def beforeAll(): Unit = {
    super.beforeAll()
    createFiles(fs, test1FilesToCreate.map(_ (test1Folder)))
    createFiles(fs, test2FilesToCreate.map(_ (test2Folder)))
    createFiles(fs, test3FilesToCreate.map(_ (test3Folder)))
    createFiles(fs, test4FilesToCreate.map(_ (test4Folder)))
  }

  override protected def afterAll(): Unit = {
    fs.delete(test1Folder, true)
    fs.delete(test2Folder, true)
    fs.delete(test3Folder, true)
    fs.delete(test4Folder, true)
    super.afterAll()
  }
}

object PartitionDiscoveryUtilsSpec {

  implicit class RichPath(val p: Path) extends AnyVal {
    def /(suffix: String): Path = {
      require(!suffix.contains("/"))
      p.suffix("/" + suffix)
    }
  }

}