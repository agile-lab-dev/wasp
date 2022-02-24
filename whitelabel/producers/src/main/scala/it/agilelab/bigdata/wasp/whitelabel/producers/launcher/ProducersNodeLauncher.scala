package it.agilelab.bigdata.wasp.whitelabel.producers.launcher

import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.utils.ConfigManager
import it.agilelab.bigdata.wasp.producers.launcher.ProducersNodeLauncherTrait
import it.agilelab.bigdata.wasp.producers.metrics.kafka.KafkaCheckOffsetsGuardian
import org.apache.commons.cli.CommandLine

object ProducersNodeLauncher extends ProducersNodeLauncherTrait {

  override def launch(commandLine: CommandLine): Unit = {
    super.launch(commandLine)
    WaspSystem.actorSystem.actorOf(
      KafkaCheckOffsetsGuardian.props(ConfigManager.getKafkaConfig),
      KafkaCheckOffsetsGuardian.name
    )
  }
}
