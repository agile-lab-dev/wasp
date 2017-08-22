package it.agilelab.bigdata.wasp.consumers.rt.launcher

import akka.actor.Props
import it.agilelab.bigdata.wasp.consumers.rt.RtConsumersMasterGuardian
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.launcher.ClusterSingletonLauncher

/**
	* Launcher for the RtConsumersMasterGuardian.
	*
	* @author Nicolò Bidotti
	*/
object RtConsumersNodeLauncher extends ClusterSingletonLauncher {
	override def getSingletonProps: Props = Props(new RtConsumersMasterGuardian(ConfigBL))
	
	override def getSingletonName: String = WaspSystem.rtConsumersMasterGuardianName
	
	override def getSingletonManagerName: String = WaspSystem.rtConsumersMasterGuardianSingletonManagerName
	
	override def getSingletonRoles: Seq[String] = Seq(WaspSystem.rtConsumersMasterGuardianRole)
	
	override def getNodeName: String = "consumers rt"
}

