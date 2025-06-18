package it.agilelab.bigdata.wasp.spark.plugins.nifi

import nifi.NifiPlugin
import org.apache.spark.api.plugin.ExecutorPlugin

trait CompatibilityNifiPlugin extends ExecutorPlugin {

  self: NifiPlugin =>
  override def init(ctx: org.apache.spark.api.plugin.PluginContext, extraConf: java.util.Map[String, String]): Unit = {
    this.initialization()
  }

}
