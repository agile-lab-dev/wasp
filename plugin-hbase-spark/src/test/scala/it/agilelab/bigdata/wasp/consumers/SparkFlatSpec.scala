package it.agilelab.bigdata.wasp.consumers

import java.nio.file.Files

import akka.actor.ActorSystem
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{ConfigMap, BeforeAndAfterAllConfigMap, BeforeAndAfter, FlatSpec}

/**
 * Created by Mattia Bertorello on 30/09/15.
 */
trait SparkFlatSpec extends FlatSpec with BeforeAndAfterAllConfigMap with ScalaFutures  {
  private val master = "local[2]"
  private val appName = "example-spark-streaming"
  private val batchDuration = Seconds(1)
  private val checkpointDir = Files.createTempDirectory(appName).toString

  protected var ssc: StreamingContext = _
  protected var sc: SparkContext = _
  protected var sqlContext: SQLContext = _
  protected var actorSystem = ActorSystem()


  override def beforeAll(cm: ConfigMap) {
    val sparkConf: SparkConf = new SparkConf()
      .setMaster("local[2]").setAppName("Instant Matching")

    ssc = new StreamingContext(sparkConf, batchDuration)

    sc = ssc.sparkContext

    sqlContext = new SQLContext(sc)
  }

  override def afterAll(cm: ConfigMap) {
    if (ssc != null) {
      ssc.awaitTerminationOrTimeout(1000)
      ssc.stop()
    }
    if (sc != null) {
      sc.stop()
      sc.clearCallSite()
    }
    actorSystem.shutdown()
  }
}