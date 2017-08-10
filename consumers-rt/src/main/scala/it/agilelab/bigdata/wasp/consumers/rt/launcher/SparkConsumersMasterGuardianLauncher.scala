package it.agilelab.bigdata.wasp.consumers.rt.launcher

import akka.actor.Props
import it.agilelab.bigdata.wasp.consumers.rt.RtConsumersMasterGuardian
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.launcher.ClusterSingletonLauncher

/**
	* Launcher for the RtConsumersMasterGuardian.
	*
	* @author Nicolò Bidotti
	*/
object SparkConsumersMasterGuardianLauncher extends ClusterSingletonLauncher {
	override def getSingletonProps: Props = {
		Props(new RtConsumersMasterGuardian(ConfigBL))
	}
	
	override def getSingletonName: String = RtConsumersMasterGuardian.name
	
	override def getSingletonRole: String = RtConsumersMasterGuardian.role
}

