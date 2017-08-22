package it.agilelab.bigdata.wasp.producers.launcher

import akka.actor.Props
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.launcher.MultipleClusterSingletonsLauncher
import it.agilelab.bigdata.wasp.producers.{InternalLogProducerGuardian, ProducersMasterGuardian}

/**
	* Launcher for the ProducersMasterGuardian and InternalLogProducerGuardian.
	*
	* @author Nicolò Bidotti
	*/
object ProducersNodeLauncher extends MultipleClusterSingletonsLauncher {
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
}