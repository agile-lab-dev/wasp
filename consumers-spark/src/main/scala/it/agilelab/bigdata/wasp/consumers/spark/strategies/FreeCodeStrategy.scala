package it.agilelab.bigdata.wasp.consumers.spark.strategies

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.consumers.spark.utils.ToolBoxUtils
import org.apache.spark.sql.DataFrame

class FreeCodeStrategy(function : String) extends Strategy {

  private lazy val _function: (Map[ReaderKey, DataFrame], Config) => DataFrame =
    ToolBoxUtils.compileCode[(Map[ReaderKey, DataFrame],Config) => DataFrame] {
    s"""
       |import it.agilelab.bigdata.wasp.consumers.spark.strategies.ReaderKey
       |import com.typesafe.config.Config
       |import org.apache.spark.sql.DataFrame
       |import org.apache.spark.sql.functions._
       |(dataFrames: Map[ReaderKey, DataFrame],configuration : Config) =>{
       |val spark = dataFrames.head._2.sparkSession
       |$function
       |}
       |""".stripMargin
  }

  def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = _function(dataFrames,configuration)

}
