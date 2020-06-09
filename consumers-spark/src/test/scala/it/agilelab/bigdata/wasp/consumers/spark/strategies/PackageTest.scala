package it.agilelab.bigdata.wasp.consumers.spark.strategies

import it.agilelab.bigdata.wasp.consumers.spark.eventengine.{FakeData, SparkSetup}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class PackageTest extends FlatSpec with Matchers with SparkSetup{

  def createDataframe(ss : SparkSession, prefix : String , range : Seq[Int]): DataFrame = {
    import ss.implicits._
    ss.sparkContext.parallelize(range).map(i=> FakeData(prefix,i.toFloat,i.toLong,i.toString,i)).toDF
  }


  it should "test getAllDataFrames" in withSparkSession { spark =>
    val map = (1 to 2).map(i=> ReaderKey(s"key_$i","2") -> createDataframe(spark,s"test_$i",1 to 10)).toMap
    val dataFrames = map.getAllDataFrames
    dataFrames.size shouldBe 2
    dataFrames.map(_.count()).sum shouldBe 20
    dataFrames.foreach(_.count() shouldBe 10)
    dataFrames.reduce(_.union(_)).where("name == 'test_1'").select("someNumber")
      .take(10).map(_.getInt(0)) should contain theSameElementsAs (1 to 10)
    dataFrames.reduce(_.union(_)).where("name == 'test_2'").select("someNumber")
      .take(10).map(_.getInt(0)) should contain theSameElementsAs (1 to 10)

  }


  it should "test getFirstDataFrame" in withSparkSession { spark =>
    val map = (1 to 2).map(i=> ReaderKey(s"key_$i","2") -> createDataframe(spark,s"test_$i",1 to 10)).toMap
    val dataFrame = map.getFirstDataFrame
    dataFrame.count() shouldBe 10

    dataFrame.where("name == 'test_1'").select("someNumber")
      .take(10).map(_.getInt(0)) should contain theSameElementsAs (1 to 10)

  }


  it should "test getAllReaderKeys" in withSparkSession { spark =>
    val map = (1 to 2).map(i=> ReaderKey(s"key_$i","2") -> createDataframe(spark,s"test_$i",1 to 10)).toMap
    val readerKeys = map.getAllReaderKeys
    readerKeys.size shouldBe 2
    readerKeys should contain theSameElementsAs (1 to 2).map(i=> ReaderKey(s"key_$i","2"))

  }


  it should "test getFirstReaderKey" in withSparkSession { spark =>
    val map = (1 to 2).map(i=> ReaderKey(s"key_$i","2") -> createDataframe(spark,s"test_$i",1 to 10)).toMap
    val readerKeys = map.getFirstReaderKey
    readerKeys shouldBe ReaderKey(s"key_1","2")

  }


  it should "test filterBySourceTypeName" in withSparkSession { spark =>
    val map = (1 to 2).map(i=> ReaderKey(s"key_$i",s"$i") -> createDataframe(spark,s"test_$i",1 to 10)).toMap
    val dataFrames = map.filterBySourceTypeName("key_2")
    dataFrames.size shouldBe 1
    dataFrames.map(_.count()).sum shouldBe 10
    dataFrames.foreach(_.count() shouldBe 10)
    dataFrames.reduce(_.union(_)).where("name == 'test_2'").select("someNumber")
      .take(10).map(_.getInt(0)) should contain theSameElementsAs (1 to 10)

  }


  it should "test filterByName" in withSparkSession { spark =>
    val map = (1 to 2).map(i=> ReaderKey(s"key_$i",s"$i") -> createDataframe(spark,s"test_$i",1 to 10)).toMap
    val dataFrames = map.filterByName("2")
    dataFrames.size shouldBe 1
    dataFrames.map(_.count()).sum shouldBe 10
    dataFrames.foreach(_.count() shouldBe 10)
    dataFrames.reduce(_.union(_)).where("name == 'test_2'").select("someNumber")
      .take(10).map(_.getInt(0)) should contain theSameElementsAs (1 to 10)

  }

  it should "test filterByName with a wrong name" in withSparkSession { spark =>
    val map = (1 to 2).map(i=> ReaderKey(s"key_$i",s"$i") -> createDataframe(spark,s"test_$i",1 to 10)).toMap
    val dataFrames = map.filterByName("9")
    dataFrames.size shouldBe 0
  }


  it should "test getByReaderKey" in withSparkSession { spark =>
    val map = (1 to 2).map(i=> ReaderKey(s"key_$i",s"$i") -> createDataframe(spark,s"test_$i",1 to 10)).toMap
    val dataFrameOption = map.getByReaderKey("key_2","2")
    dataFrameOption.isDefined shouldBe true
    val dataFrame = dataFrameOption.get
    dataFrame.count() shouldBe 10
    dataFrame.where("name == 'test_2'").select("someNumber")
      .take(10).map(_.getInt(0)) should contain theSameElementsAs (1 to 10)

  }



  it should "test getByReaderKey wrong" in withSparkSession { spark =>
    val map = (1 to 2).map(i=> ReaderKey(s"key_$i",s"$i") -> createDataframe(spark,s"test_$i",1 to 10)).toMap
    map.getByReaderKey("key_2","9").isDefined shouldBe false
    map.getByReaderKey("key_9","2").isDefined shouldBe false
  }




}
