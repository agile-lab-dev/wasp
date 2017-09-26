package it.agilelab.bigdata.wasp.producers

import it.agilelab.bigdata.wasp.core.models.ProducerModel
import it.agilelab.bigdata.wasp.producers.launcher.ProducersNodeLauncherTrait

/**
  * Launcher for the NifiProducer.
  *
  * @author Alessandro Marino
  */

object NifiProducersNodeLauncher extends ProducersNodeLauncherTrait {
  override def launch(args: Array[String]): Unit = {
    super.launch(args)
    waspDB.insertIfNotExists[ProducerModel](NifiProducerModel.nifiProducer)
  }
}
