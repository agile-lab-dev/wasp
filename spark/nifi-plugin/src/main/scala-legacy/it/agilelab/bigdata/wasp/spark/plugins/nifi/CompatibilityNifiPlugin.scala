package it.agilelab.bigdata.wasp.spark.plugins.nifi

import nifi.NifiPlugin
import org.apache.spark.ExecutorPlugin

trait CompatibilityNifiPlugin extends ExecutorPlugin {

  self: NifiPlugin =>
  override def init(): Unit = {
    this.initialization()
  }

}