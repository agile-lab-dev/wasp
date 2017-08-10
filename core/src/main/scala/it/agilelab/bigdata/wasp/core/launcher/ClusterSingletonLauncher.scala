package it.agilelab.bigdata.wasp.core.launcher

import akka.actor.{PoisonPill, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import it.agilelab.bigdata.wasp.core.WaspSystem

/**
	* Launcher for cluster singleton actor.
	*
	* @author Nicolò Bidotti
	*/
trait ClusterSingletonLauncher extends WaspLauncher {
	override def launch(args: Array[String]): Unit = {
		val actorSystem = WaspSystem.actorSystem
		
		// spawn the cluster singleton manager, which will spawn the actual singleton actor as defined by the getters
		actorSystem.actorOf(
			ClusterSingletonManager.props(
				singletonProps = getSingletonProps,
				terminationMessage = PoisonPill,
				settings = ClusterSingletonManagerSettings(actorSystem).withRole(getSingletonRole)
			),
			name = getSingletonName
		)
	}
	
	def getSingletonProps: Props
	
	def getSingletonName: String
	
	def getSingletonRole: String
}
