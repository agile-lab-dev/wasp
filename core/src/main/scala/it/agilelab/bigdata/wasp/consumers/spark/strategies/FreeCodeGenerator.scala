package it.agilelab.bigdata.wasp.consumers.spark.strategies
import FreeCodeGenerator._

trait FreeCodeGenerator {

  def startRowCode: Int = defaultStartFreeCodeAsFunction.split("\n").length


  def completeWithDefaultCodeAsFunction(code : String): String = {
    s"""$defaultStartFreeCodeAsFunction
       |$code
       |$defaultEndFreeCodeTransform""".stripMargin
  }


  def startPosition : Int = defaultStartFreeCodeAsFunction.length+1

}
object FreeCodeGenerator {
  val defaultStartFreeCodeAsFunction: String=
    """import it.agilelab.bigdata.wasp.consumers.spark.strategies.ReaderKey
      |import it.agilelab.bigdata.wasp.consumers.spark.strategies._
      |import com.typesafe.config.Config
      |import org.apache.spark.sql.DataFrame
      |import org.apache.spark.sql.functions._
      |((dataFrames: Map[ReaderKey, DataFrame],configuration : Config) =>{
      |val spark = dataFrames.head._2.sparkSession""".stripMargin


  val defaultEndFreeCodeTransform : String= "}) : ((Map[ReaderKey, DataFrame], Config) => DataFrame) "



}

