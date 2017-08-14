package it.agilelab.bigdata.wasp.producers.launcher

import akka.actor.Props
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.launcher.ClusterSingletonLauncher
import it.agilelab.bigdata.wasp.producers.ProducersMasterGuardian

/**
	* Launcher for the ProducersMasterGuardian.
	*
	* @author Nicolò Bidotti
	*/
object ProducerMasterGuardianLauncher extends ClusterSingletonLauncher {
	override def getSingletonProps: Props = {
		Props(new ProducersMasterGuardian(ConfigBL))
	}
	
	override def getSingletonName: String = ProducersMasterGuardian.name
	
	override def getSingletonRole: String = ProducersMasterGuardian.role
}
