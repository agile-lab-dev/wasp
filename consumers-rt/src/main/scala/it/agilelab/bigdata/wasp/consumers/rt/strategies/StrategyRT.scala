package it.agilelab.bigdata.wasp.consumers.rt.strategies

import com.typesafe.config.Config

trait StrategyRT {

  var configuration: Config

  //def transform(topic_name: String, input: Array[Byte]): Array[Byte]
  def transform(topic_name: String, input: String): String

}
