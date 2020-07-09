package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools.FolderCompactionUtils._
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.raw.tools.WhereCondition
import it.agilelab.bigdata.wasp.models.{RawModel, RawOptions}

class FolderCompactionUtilsSpec extends FlatSpec with Matchers with BeforeAndAfterEach with SparkSuite {

  private val configString =
    """{
      |  "inputModel" : "name of the input model",
      |  "outputModel" : "name of the input model",
      |  "partitions" : {
      |    "columnName1" : ["value1", "value2"],
      |    "columnName2" : ["value3", "value4"]
      |  },
      |  "numPartitions" : 1
      |}""".stripMargin

  private val config = ConfigFactory.parseString(configString)

  it should "generate the correct combinations" in {
    val input = List(
      "a" -> List("1", "2", "3"),
      "b" -> List("9", "8"),
      "c" -> List("4", "5", "6")
    )
    val output = FolderCompactionUtils.generateCombinations(input)
    output.toSet.toList should contain theSameElementsAs output

    val expectedTuples = List(
      ("a", "1") :: ("b", "9") :: ("c", "4") :: Nil,
      ("a", "1") :: ("b", "9") :: ("c", "5") :: Nil,
      ("a", "1") :: ("b", "9") :: ("c", "6") :: Nil,
      ("a", "1") :: ("b", "8") :: ("c", "4") :: Nil,
      ("a", "1") :: ("b", "8") :: ("c", "5") :: Nil,
      ("a", "1") :: ("b", "8") :: ("c", "6") :: Nil,
      ("a", "2") :: ("b", "9") :: ("c", "4") :: Nil,
      ("a", "2") :: ("b", "9") :: ("c", "5") :: Nil,
      ("a", "2") :: ("b", "9") :: ("c", "6") :: Nil,
      ("a", "2") :: ("b", "8") :: ("c", "4") :: Nil,
      ("a", "2") :: ("b", "8") :: ("c", "5") :: Nil,
      ("a", "2") :: ("b", "8") :: ("c", "6") :: Nil,
      ("a", "3") :: ("b", "9") :: ("c", "4") :: Nil,
      ("a", "3") :: ("b", "9") :: ("c", "5") :: Nil,
      ("a", "3") :: ("b", "9") :: ("c", "6") :: Nil,
      ("a", "3") :: ("b", "8") :: ("c", "4") :: Nil,
      ("a", "3") :: ("b", "8") :: ("c", "5") :: Nil,
      ("a", "3") :: ("b", "8") :: ("c", "6") :: Nil
    )

    val expected = expectedTuples.map(_.map(x => Partition(x._1, x._2)))

    expected should contain theSameElementsInOrderAs output
  }

  it should "generate the correct where conditions 1" in {
    val inPartitions = List("a", "b")
    val outPartitions = List("a")

    val partitions = Map(
      "a" -> List("1", "2"),
      "b" -> List("3", "4", "5"),
      "c" -> List("6", "7", "8", "9")
    )

    val inputOptions = RawOptions(saveMode = "spark save mode",
      format = "spark data format",
      extraOptions = None,
      partitionBy = Some(inPartitions))
    val outputOptions = inputOptions.copy(partitionBy = Some(outPartitions))
    val inputModel = RawModel(name = "inModel", uri = "uri", schema = "{}", options = inputOptions)
    val outputModel = RawModel(name = "outModel", uri = "uri", schema = "{}", options = outputOptions)

    val expectedQueries = List(
      col("a").equalTo(lit("1"))
        .and(lit(false)
          .or(col("b").equalTo(lit("3")))
          .or(col("b").equalTo(lit("4")))
          .or(col("b").equalTo(lit("5")))),
      col("a").equalTo(lit("2"))
        .and(lit(false)
          .or(col("b").equalTo(lit("3")))
          .or(col("b").equalTo(lit("4")))
          .or(col("b").equalTo(lit("5"))))
    )

    generateWhereConditions(partitions, inputModel, outputModel).map(_.toSparkColumn) should contain theSameElementsAs expectedQueries
  }

  it should "generate the correct where conditions 2" in {
    val inPartitions = List("a", "b", "c")
    val outPartitions = List("a", "b")

    val partitions = Map(
      "a" -> List("1", "2"),
      "b" -> List("3", "4", "5"),
      "c" -> List("6", "7", "8", "9")
    )

    val inputOptions = RawOptions(saveMode = "spark save mode",
      format = "spark data format",
      extraOptions = None,
      partitionBy = Some(inPartitions))
    val outputOptions = inputOptions.copy(partitionBy = Some(outPartitions))
    val inputModel = RawModel(name = "inModel", uri = "uri", schema = "{}", options = inputOptions)
    val outputModel = RawModel(name = "outModel", uri = "uri", schema = "{}", options = outputOptions)

    val expectedQueries = List(
      lit(true)
        .and(col("a").equalTo(lit("1")))
        .and(col("b").equalTo(lit("3")))
        .and(lit(false)
          .or(col("c").equalTo(lit("6")))
          .or(col("c").equalTo(lit("7")))
          .or(col("c").equalTo(lit("8")))
          .or(col("c").equalTo(lit("9")))),
      lit(true)
        .and(col("a").equalTo(lit("1")))
        .and(col("b").equalTo(lit("4")))
        .and(lit(false)
          .or(col("c").equalTo(lit("6")))
          .or(col("c").equalTo(lit("7")))
          .or(col("c").equalTo(lit("8")))
          .or(col("c").equalTo(lit("9")))),
      lit(true)
        .and(col("a").equalTo(lit("1")))
        .and(col("b").equalTo(lit("5")))
        .and(lit(false)
          .or(col("c").equalTo(lit("6")))
          .or(col("c").equalTo(lit("7")))
          .or(col("c").equalTo(lit("8")))
          .or(col("c").equalTo(lit("9")))),
      lit(true)
        .and(col("a").equalTo(lit("2")))
        .and(col("b").equalTo(lit("3")))
        .and(lit(false)
          .or(col("c").equalTo(lit("6")))
          .or(col("c").equalTo(lit("7")))
          .or(col("c").equalTo(lit("8")))
          .or(col("c").equalTo(lit("9")))),
      lit(true)
        .and(col("a").equalTo(lit("2")))
        .and(col("b").equalTo(lit("4")))
        .and(lit(false)
          .or(col("c").equalTo(lit("6")))
          .or(col("c").equalTo(lit("7")))
          .or(col("c").equalTo(lit("8")))
          .or(col("c").equalTo(lit("9")))),
      lit(true)
        .and(col("a").equalTo(lit("2")))
        .and(col("b").equalTo(lit("5")))
        .and(lit(false)
          .or(col("c").equalTo(lit("6")))
          .or(col("c").equalTo(lit("7")))
          .or(col("c").equalTo(lit("8")))
          .or(col("c").equalTo(lit("9"))))
    )

    generateWhereConditions(partitions, inputModel, outputModel).map(_.toSparkColumn) should contain theSameElementsAs expectedQueries
  }

  it should "generate the correct where conditions 3" in {
    val inPartitions = List("a", "b", "c")
    val outPartitions = List("a")

    val partitions = Map(
      "a" -> List("1", "2"),
      "b" -> List("3", "4", "5"),
      "c" -> List("6", "7", "8", "9")
    )

    val inputOptions = RawOptions(saveMode = "spark save mode",
      format = "spark data format",
      extraOptions = None,
      partitionBy = Some(inPartitions))
    val outputOptions = inputOptions.copy(partitionBy = Some(outPartitions))
    val inputModel = RawModel(name = "inModel", uri = "uri", schema = "{}", options = inputOptions)
    val outputModel = RawModel(name = "outModel", uri = "uri", schema = "{}", options = outputOptions)

    val expectedQueries = List(
      col("a").equalTo(lit("1"))
        .and(
          lit(false)
            .or(lit(true)
              .and(col("b").equalTo(lit("3")))
              .and(col("c").equalTo(lit("6"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("3")))
              .and(col("c").equalTo(lit("7"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("3")))
              .and(col("c").equalTo(lit("8"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("3")))
              .and(col("c").equalTo(lit("9"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("4")))
              .and(col("c").equalTo(lit("6"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("4")))
              .and(col("c").equalTo(lit("7"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("4")))
              .and(col("c").equalTo(lit("8"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("4")))
              .and(col("c").equalTo(lit("9"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("5")))
              .and(col("c").equalTo(lit("6"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("5")))
              .and(col("c").equalTo(lit("7"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("5")))
              .and(col("c").equalTo(lit("8"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("5")))
              .and(col("c").equalTo(lit("9"))))
        ),
      col("a").equalTo(lit("2"))
        .and(
          lit(false)
            .or(lit(true)
              .and(col("b").equalTo(lit("3")))
              .and(col("c").equalTo(lit("6"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("3")))
              .and(col("c").equalTo(lit("7"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("3")))
              .and(col("c").equalTo(lit("8"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("3")))
              .and(col("c").equalTo(lit("9"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("4")))
              .and(col("c").equalTo(lit("6"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("4")))
              .and(col("c").equalTo(lit("7"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("4")))
              .and(col("c").equalTo(lit("8"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("4")))
              .and(col("c").equalTo(lit("9"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("5")))
              .and(col("c").equalTo(lit("6"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("5")))
              .and(col("c").equalTo(lit("7"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("5")))
              .and(col("c").equalTo(lit("8"))))
            .or(lit(true)
              .and(col("b").equalTo(lit("5")))
              .and(col("c").equalTo(lit("9"))))
        )
    )

    generateWhereConditions(partitions, inputModel, outputModel).map(_.toSparkColumn) should contain theSameElementsAs expectedQueries
  }

  it should "generate the correct where conditions 4" in {
    val inPartitions = List("a", "b", "c")
    val outPartitions = List("c")

    val partitions = Map(
      "a" -> List("1", "2"),
      "b" -> List("3", "4", "5"),
      "c" -> List("6", "7", "8", "9")
    )

    val inputOptions = RawOptions(saveMode = "spark save mode",
      format = "spark data format",
      extraOptions = None,
      partitionBy = Some(inPartitions))
    val outputOptions = inputOptions.copy(partitionBy = Some(outPartitions))
    val inputModel = RawModel(name = "inModel", uri = "uri", schema = "{}", options = inputOptions)
    val outputModel = RawModel(name = "outModel", uri = "uri", schema = "{}", options = outputOptions)

    val expectedQueries = List(
      col("c").equalTo(lit("6"))
        .and(
          lit(false)
            .or(lit(true)
              .and(col("a").equalTo(lit("1")))
              .and(col("b").equalTo(lit("3"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("1")))
              .and(col("b").equalTo(lit("4"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("1")))
              .and(col("b").equalTo(lit("5"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("2")))
              .and(col("b").equalTo(lit("3"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("2")))
              .and(col("b").equalTo(lit("4"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("2")))
              .and(col("b").equalTo(lit("5"))))
        ),
      col("c").equalTo(lit("7"))
        .and(
          lit(false)
            .or(lit(true)
              .and(col("a").equalTo(lit("1")))
              .and(col("b").equalTo(lit("3"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("1")))
              .and(col("b").equalTo(lit("4"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("1")))
              .and(col("b").equalTo(lit("5"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("2")))
              .and(col("b").equalTo(lit("3"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("2")))
              .and(col("b").equalTo(lit("4"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("2")))
              .and(col("b").equalTo(lit("5"))))
        ),
      col("c").equalTo(lit("8"))
        .and(
          lit(false)
            .or(lit(true)
              .and(col("a").equalTo(lit("1")))
              .and(col("b").equalTo(lit("3"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("1")))
              .and(col("b").equalTo(lit("4"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("1")))
              .and(col("b").equalTo(lit("5"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("2")))
              .and(col("b").equalTo(lit("3"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("2")))
              .and(col("b").equalTo(lit("4"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("2")))
              .and(col("b").equalTo(lit("5"))))
        ),
      col("c").equalTo(lit("9"))
        .and(
          lit(false)
            .or(lit(true)
              .and(col("a").equalTo(lit("1")))
              .and(col("b").equalTo(lit("3"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("1")))
              .and(col("b").equalTo(lit("4"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("1")))
              .and(col("b").equalTo(lit("5"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("2")))
              .and(col("b").equalTo(lit("3"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("2")))
              .and(col("b").equalTo(lit("4"))))
            .or(lit(true)
              .and(col("a").equalTo(lit("2")))
              .and(col("b").equalTo(lit("5"))))
        )
    )

    generateWhereConditions(partitions, inputModel, outputModel).map(_.toSparkColumn) should contain theSameElementsAs expectedQueries
  }

  it should "generate the correct where conditions for empty output partitions" in {
    val inPartitions = List("a", "b")
    val outPartitions = List()

    val partitions = Map(
      "a" -> List("1", "2"),
      "b" -> List("3", "4", "5"),
      "c" -> List("6", "7", "8", "9")
    )

    val inputOptions = RawOptions(saveMode = "spark save mode",
      format = "spark data format",
      extraOptions = None,
      partitionBy = Some(inPartitions))
    val outputOptions = inputOptions.copy(partitionBy = Some(outPartitions))
    val inputModel = RawModel(name = "inModel", uri = "uri", schema = "{}", options = inputOptions)
    val outputModel = RawModel(name = "outModel", uri = "uri", schema = "{}", options = outputOptions)

    val expectedQueries = List(
      lit(true)
        .and(lit(false)
          .or(lit(true)
            .and(col("a").equalTo(lit("1")))
            .and(col("b").equalTo(lit("3"))))
          .or(lit(true)
            .and(col("a").equalTo(lit("1")))
            .and(col("b").equalTo(lit("4"))))
          .or(lit(true)
            .and(col("a").equalTo(lit("1")))
            .and(col("b").equalTo(lit("5"))))
          .or(lit(true)
            .and(col("a").equalTo(lit("2")))
            .and(col("b").equalTo(lit("3"))))
          .or(lit(true)
            .and(col("a").equalTo(lit("2")))
            .and(col("b").equalTo(lit("4"))))
          .or(lit(true)
            .and(col("a").equalTo(lit("2")))
            .and(col("b").equalTo(lit("5"))))
        )
    )

    generateWhereConditions(partitions, inputModel, outputModel).map(_.toSparkColumn) should contain theSameElementsAs expectedQueries
  }

  it should "generate the correct where conditions for the same input and output columns" in {
    val inPartitions = List("a", "b")
    val outPartitions = List("a", "b")

    val partitions = Map(
      "a" -> List("1", "2"),
      "b" -> List("3", "4", "5"),
      "c" -> List("6", "7", "8", "9")
    )

    val inputOptions = RawOptions(saveMode = "spark save mode",
      format = "spark data format",
      extraOptions = None,
      partitionBy = Some(inPartitions))
    val outputOptions = inputOptions.copy(partitionBy = Some(outPartitions))
    val inputModel = RawModel(name = "inModel", uri = "uri", schema = "{}", options = inputOptions)
    val outputModel = RawModel(name = "outModel", uri = "uri", schema = "{}", options = outputOptions)

    val expectedQueries = List(
      lit(true)
        .and(col("a").equalTo(lit("1")))
        .and(col("b").equalTo(lit("3")))
        .and(lit(true)),
      lit(true)
        .and(col("a").equalTo(lit("1")))
        .and(col("b").equalTo(lit("4")))
        .and(lit(true)),
      lit(true)
        .and(col("a").equalTo(lit("2")))
        .and(col("b").equalTo(lit("5")))
        .and(lit(true)),
      lit(true)
        .and(col("a").equalTo(lit("2")))
        .and(col("b").equalTo(lit("3")))
        .and(lit(true)),
      lit(true)
        .and(col("a").equalTo(lit("2")))
        .and(col("b").equalTo(lit("4")))
        .and(lit(true)),
      lit(true)
        .and(col("a").equalTo(lit("1")))
        .and(col("b").equalTo(lit("5")))
        .and(lit(true))
    )

    generateWhereConditions(partitions, inputModel, outputModel).map(_.toSparkColumn) should contain theSameElementsAs expectedQueries
  }

  it should "generate the correct RawModel from configuration" in {
    val modelConfigString =
      """{
        |  name: modelName
        |  uri: uri
        |  schema: "{ 'val1': int, 'val2': bool }"
        |  timed: false
        |  options: {
        |    saveMode: spark save mode
        |    format: spark data format
        |    extraOptions: {
        |      key: value
        |    }
        |    partitionBy: [
        |      partitionColumn1,
        |      partitionColumn2
        |    ]
        |  }
        |}""".stripMargin
    val modelConfig = ConfigFactory.parseString(modelConfigString)

    val expectedRawOption = RawOptions(saveMode = "spark save mode",
      format = "spark data format",
      extraOptions = Some(Map("key" -> "value")),
      partitionBy = Some("partitionColumn1" :: "partitionColumn2" :: Nil))
    val expectedModel = RawModel(name = "modelName", uri = "uri", schema = "{ 'val1': int, 'val2': bool }", options = expectedRawOption, timed = false)

    FolderCompactionUtils.parseConfigModel(modelConfig) shouldBe expectedModel
  }

  it should "parse the configuration partitions correctly" in {
    val expectedPartitions = Map("columnName1" -> List("value1", "value2"), "columnName2" -> List("value3", "value4"))
    FolderCompactionUtils.parsePartitions(config) should contain theSameElementsAs expectedPartitions
  }

  it should "correctly filter an existent file" in {
    val files = List(
      new Path("a=1/b=2/c=2/d=1"),
      new Path("a=1/b=2/c=2/d=1"),
      new Path("a=1/b=2/c=2/d=1"),
      new Path("a=1/b=2/c=2/d=1"),
      new Path("a=1/b=2/c=2")
    )

    val outPartitions = Partition("a=1") :: Partition("b=2") :: Nil

    val inPartitions1 = Partition("c=1") :: Partition("d=1") :: Nil
    val inPartitions2 = Partition("c=1") :: Partition("d=2") :: Nil
    val inPartitions3 = Partition("c=2") :: Partition("d=1") :: Nil
    val inPartitions4 = Partition("c=2") :: Partition("d=2") :: Nil

    val whereCondition = WhereCondition(outPartitions, List(inPartitions1, inPartitions2, inPartitions3, inPartitions4))

    filterWhereCondition(files)(whereCondition) shouldBe true
  }

  it should "correctly filter an existent file 2" in {
    val files = List(
      new Path("a=1/b=2/c=2/d=4"),
      new Path("a=1/b=2/c=2/d=4"),
      new Path("a=1/b=2/c=2/d=4"),
      new Path("a=1/b=2/c=2/d=4")
    )

    val outPartitions = Partition("a=1") :: Partition("b=2") :: Nil

    val inPartitions1 = Partition("c=1") :: Nil
    val inPartitions2 = Partition("c=2") :: Nil

    val whereCondition = WhereCondition(outPartitions, List(inPartitions1, inPartitions2))

    filterWhereCondition(files)(whereCondition) shouldBe true
  }

  it should "correctly filter a non existent file" in {
    val files = List(
      new Path("a=3/b=2/c=2/d=1"),
      new Path("a=3/b=2/c=2/d=1"),
      new Path("a=3/b=2/c=2/d=1"),
      new Path("a=3/b=2/c=2/d=1"),
      new Path("a=13/b=2/c=2")
    )

    val outPartitions = Partition("a=1") :: Partition("b=2") :: Nil

    val inPartitions1 = Partition("c=1") :: Partition("d=1") :: Nil
    val inPartitions2 = Partition("c=1") :: Partition("d=2") :: Nil
    val inPartitions3 = Partition("c=2") :: Partition("d=1") :: Nil
    val inPartitions4 = Partition("c=2") :: Partition("d=2") :: Nil

    val whereCondition = WhereCondition(outPartitions, List(inPartitions1, inPartitions2, inPartitions3, inPartitions4))

    filterWhereCondition(files)(whereCondition) shouldBe false
  }

  it should "correctly filter an existent file 3" in {
    val files = List(
      new Path("a=3/b=2/c=2/d=1"),
      new Path("a=3/b=2/c=2/d=1"),
      new Path("a=3/b=2/c=2/d=1"),
      new Path("a=3/b=2/c=2/d=1"),
      new Path("a=1/b=2/c=2")
    )

    val outPartitions = Partition("a=1") :: Partition("b=2") :: Nil

    val whereCondition = WhereCondition(outPartitions, List())

    filterWhereCondition(files)(whereCondition) shouldBe true
  }

}

object FolderCompactionUtilsSpec {

  implicit class RichPath(val p: Path) extends AnyVal {
    def /(suffix: String): Path = {
      require(!suffix.contains("/"))
      p.suffix("/" + suffix)
    }
  }

}


