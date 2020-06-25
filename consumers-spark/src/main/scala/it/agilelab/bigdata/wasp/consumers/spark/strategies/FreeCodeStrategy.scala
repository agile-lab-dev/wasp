package it.agilelab.bigdata.wasp.consumers.spark.strategies

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.consumers.spark.utils.ToolBoxUtils
import org.apache.spark.sql.DataFrame

class FreeCodeStrategy(function : String) extends InternalStrategy with FreeCodeGenerator {

  private lazy val _function: (Map[ReaderKey, DataFrame], Config) => DataFrame =
    ToolBoxUtils.compileCode[(Map[ReaderKey, DataFrame],Config) => DataFrame] {
      completeWithDefaultCodeAsFunction(function)
  }

  def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = _function(dataFrames,configuration)

}
