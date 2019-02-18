package it.agilelab.bigdata.wasp.consumers.spark.plugins.mailer

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{Matchers, WordSpec}

import scala.util.Try

//class InnerMailWriterSpec
//  extends WordSpec with Matchers with SparkSetup {
//
//  val target = new InnerMailWriter(new MailAgentFake())
//
//  val mailSeq: Seq[Mail] = Range(1, 10).map(_ => Mail("receiver", Some("cc"), Some("bcc"), "subject", "content"))
//  val notMailSeq: Seq[NotMail] = Range(1, 10).map(_ => NotMail("field1", "field2", "field3"))
//
//  "An incorrect input schema" should {
//    "throw exception" in withSparkSession { ss => {
//      import ss.implicits._
//      val notMailDf = ss.sparkContext.parallelize(notMailSeq).toDF
//
//      val res = Try{target.write(notMailDf)}
//      res.isFailure should be (true)
//      res.failed.get.isInstanceOf[UnsupportedOperationException] should be (true)
//    }}
//  }
//
//  "A correct input schema" should {
//    "not explode" in withSparkSession { ss => {
//      import ss.implicits._
//      val mailDf = ss.sparkContext.parallelize(mailSeq).toDF
//
//      val res = Try{target.write(mailDf)}
//      res.isSuccess should be (true)
//    }}
//  }
//
//}
//
///**
//  * This trait allows to use spark in Unit tests
//  * https://stackoverflow.com/questions/43729262/how-to-write-unit-tests-in-spark-2-0
//  */
//trait SparkSetup {
//
//  def withSparkSession(testMethod: SparkSession => Any): Unit = {
//    val conf = new SparkConf()
//      .setMaster("local")
//      .setAppName("Spark test")
//    val sparkSession = SparkSession
//      .builder()
//      .config(conf)
//      .getOrCreate()
//
//    testMethod(sparkSession)
//    // finally sparkSession.stop()
//  }
//}
