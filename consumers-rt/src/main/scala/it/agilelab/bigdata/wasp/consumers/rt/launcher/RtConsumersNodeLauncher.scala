package it.agilelab.bigdata.wasp.consumers.rt.launcher

import akka.actor.Props
import it.agilelab.bigdata.wasp.consumers.rt.RtConsumersMasterGuardian
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.launcher.ClusterSingletonLauncher
import org.apache.commons.cli.CommandLine

/**
	* Launcher for the RtConsumersMasterGuardian.
	* This trait is useful for who want extend the launcher
	*
	* @author Nicolò Bidotti
	*/
trait RtConsumersNodeLauncherTrait extends ClusterSingletonLauncher {
  override def getSingletonProps: Props = Props(new RtConsumersMasterGuardian(ConfigBL))

  override def getSingletonName: String = WaspSystem.rtConsumersMasterGuardianName

  override def getSingletonManagerName: String = WaspSystem.rtConsumersMasterGuardianSingletonManagerName

  override def getSingletonRoles: Seq[String] = Seq(WaspSystem.rtConsumersMasterGuardianRole)

  override def getNodeName: String = "consumers rt"

  override protected def shouldDropDb(commandLine: CommandLine): Boolean = false
}

/**
	*
	* Create the main static method to run
	*/
object RtConsumersNodeLauncher extends RtConsumersNodeLauncherTrait
