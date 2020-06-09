package it.agilelab.bigdata.wasp.consumers.spark.strategies

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}
import it.agilelab.bigdata.wasp.consumers.spark.eventengine.{FakeData, SparkSetup}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class FreeCodeStrategyTest extends FlatSpec with Matchers with SparkSetup {


  def createDataframe(ss: SparkSession, prefix: String, range: Seq[Int]): DataFrame = {
    import ss.implicits._
    ss.sparkContext.parallelize(range).map(i => FakeData(prefix, i.toFloat, i.toLong, i.toString, i)).toDF
  }

  it should "test create a reflection strategy" in withSparkSession { ss =>

    val map = (1 to 2).map(i => ReaderKey(s"key_$i", "2") -> createDataframe(ss, s"test_$i", 1 to 10)).toMap
    val strategy = new FreeCodeStrategy(
      """
        |import it.agilelab.bigdata.wasp.consumers.spark.strategies.TestObj._
        |val df = dataFrames.getFirstDataFrame.select("name","someNumber","someLong")
        |val udfEven = spark.udf.register("udfEven",even )
        |df.withColumn("someNumber",df("someNumber")*df("someNumber"))
        |  .withColumn("extra",lit("TEST"))
        |  .withColumn("extra_bis",lit(value_1))
        |  .withColumn("even",udfEven(df("someLong")))
        |""".stripMargin)

    val df = strategy.transform(map).cache()

    df.count() shouldBe 10
    df.select("name").take(10).map(_.getString(0)).foreach(p => p shouldBe "test_1")
    df.select("someNumber").take(10).map(_.getInt(0)) should contain theSameElementsAs (1 to 10).map(i => i * i)
    df.select("extra").take(10).map(_.getString(0)).foreach(p => p shouldBe "TEST")
    df.select("extra_bis").take(10).map(_.getString(0)).foreach(p => p shouldBe TestObj.value_1)
    df.take(10).map(e => (e.getAs[Boolean]("even"), e.getAs[Int]("someNumber")))
      .foreach(p => p._2 % 2 == 0 shouldBe p._1)

  }


  it should "test using the config on reflation" in withSparkSession { ss =>

    val map = (1 to 2).map(i => ReaderKey(s"key_$i", "2") -> createDataframe(ss, s"test_$i", 1 to 10)).toMap
    val strategy = new FreeCodeStrategy(
      """
        |
        |val df = dataFrames.getFirstDataFrame.select("name","someNumber","someLong")
        |val s = if(configuration.isEmpty) "null" else configuration.getString("city")
        |
        |df.withColumn("someNumber",df("someNumber")*df("someNumber"))
        |  .withColumn("city",lit(s))
        |""".stripMargin)

    val df = strategy.transform(map).cache()
    df.count() shouldBe 10
    df.select("name").take(10).map(_.getString(0)).foreach(p => p shouldBe "test_1")
    df.select("someNumber").take(10).map(_.getInt(0)) should contain theSameElementsAs (1 to 10).map(i => i * i)
    df.select("city").take(10).map(_.getString(0)).foreach(p => p shouldBe "null")


    val p = new Properties()
    p.setProperty("city", "Rome")
    strategy.configuration = ConfigFactory.parseProperties(p)


    val df2 = strategy.transform(map).cache()
    df2.count() shouldBe 10
    df2.select("name").take(10).map(_.getString(0)).foreach(p => p shouldBe "test_1")
    df2.select("someNumber").take(10).map(_.getInt(0)) should contain theSameElementsAs (1 to 10).map(i => i * i)
    df2.select("city").take(10).map(_.getString(0)).foreach(p => p shouldBe "Rome")

  }


}
object TestObj {
  val value_1 = "Hello"
  val  even: Int => Boolean = (int : Int) => int%2==0

}
