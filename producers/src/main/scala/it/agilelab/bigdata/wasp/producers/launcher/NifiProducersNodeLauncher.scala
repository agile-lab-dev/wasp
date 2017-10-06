package it.agilelab.bigdata.wasp.producers.launcher

import it.agilelab.bigdata.wasp.core.models.{MlModelOnlyInfo, ProducerModel}
import it.agilelab.bigdata.wasp.producers.NifiProducerModel

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