package it.agilelab.bigdata.wasp.producers.launcher

import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.models.{MlModelOnlyInfo, ProducerModel}
import it.agilelab.bigdata.wasp.producers.NifiProducerModel
import org.apache.commons.cli.CommandLine

/**
  * Launcher for the NifiProducer.
  *
  * @author Alessandro Marino
  */

object NifiProducersNodeLauncher extends ProducersNodeLauncherTrait {

  override def launch(commandLine: CommandLine): Unit = {
    super.launch(commandLine)
    ConfigBL.producerBL.insertIfNotExists(NifiProducerModel.nifiProducer)
  }
}