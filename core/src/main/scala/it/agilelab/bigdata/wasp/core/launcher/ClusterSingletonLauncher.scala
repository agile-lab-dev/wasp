package it.agilelab.bigdata.wasp.core.launcher

import akka.actor.Props

/**
	* Launcher for a single cluster singleton actor.
	*
	* @author Nicolò Bidotti
	*/
trait ClusterSingletonLauncher extends MultipleClusterSingletonsLauncher {
	override final def getSingletonInfos: Seq[(Props, String, Seq[String])] = {
		Seq((getSingletonProps, getSingletonName, getSingletonRoles))
	}
	
	def getSingletonProps: Props
	
	def getSingletonName: String
	
	def getSingletonRoles: Seq[String]
}
