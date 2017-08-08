package it.agilelab.bigdata.wasp.consumers.rt.strategies

trait StrategyRT {

  var configuration = Map[String, Any]()

  //def transform(topic_name: String, input: Array[Byte]): Array[Byte]
  def transform(topic_name: String, input: String): String

}
