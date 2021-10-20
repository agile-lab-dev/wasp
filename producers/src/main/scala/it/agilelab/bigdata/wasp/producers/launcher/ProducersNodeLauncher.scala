package it.agilelab.bigdata.wasp.producers.launcher

import akka.actor.Props
import it.agilelab.bigdata.wasp.core.launcher.MultipleClusterSingletonsLauncher
import it.agilelab.bigdata.wasp.core.{AroundLaunch, WaspSystem}
import it.agilelab.bigdata.wasp.producers.{InternalLogProducerGuardian, ProducersMasterGuardian}
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import org.apache.commons.cli.CommandLine

/**
	* Launcher for the ProducersMasterGuardian and InternalLogProducerGuardian (only with systemproducers.start = true).
	* This trait is useful for who want extend the launcher
	* @author Nicolò Bidotti
	*/
trait ProducersNodeLauncherTrait extends MultipleClusterSingletonsLauncher with AroundLaunch {

  def beforeLaunch(): Unit = ()

  def afterLaunch(): Unit = ()

  override def launch(commandLine: CommandLine): Unit = {
    beforeLaunch()
    super.launch(commandLine)
    afterLaunch()
  }

  override def getSingletonInfos: Seq[(Props, String, String, Seq[String])] = {
    val producersMasterGuardianSingletonInfo = (
      Props(new ProducersMasterGuardian(ConfigBL)),
      WaspSystem.producersMasterGuardianName,
      WaspSystem.producersMasterGuardianSingletonManagerName,
      Seq(WaspSystem.producersMasterGuardianRole)
    )

    val loggerActorSingletonInfo = (
      Props(new InternalLogProducerGuardian(ConfigBL)),
      WaspSystem.loggerActorName,
      WaspSystem.loggerActorSingletonManagerName,
      Seq(WaspSystem.loggerActorRole)
    )

    Seq(producersMasterGuardianSingletonInfo, loggerActorSingletonInfo)
  }

  override def getNodeName: String = "producers"

  override protected def shouldDropDb(commandLine: CommandLine): Boolean = false
}

/**
	*
	* Create the main static method to run
	*/
object ProducersNodeLauncher extends ProducersNodeLauncherTrait
