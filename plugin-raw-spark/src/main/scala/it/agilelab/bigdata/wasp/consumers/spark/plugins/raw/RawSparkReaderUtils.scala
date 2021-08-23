package it.agilelab.bigdata.wasp.consumers.spark.plugins.raw

object RawSparkReaderUtils {
  def printExtraOptions(modelName: String, extraOptions: Map[String, String]): String = {
    if (extraOptions.isEmpty) {
      s"Not pushing any extra option for model named ${modelName}"
    } else {
      s"Pushing extra options for model named ${modelName}:\n" + extraOptions.mkString("* ", "\n*", "")
    }
  }
}
