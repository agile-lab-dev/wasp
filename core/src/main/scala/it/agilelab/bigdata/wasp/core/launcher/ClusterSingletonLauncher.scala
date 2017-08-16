package it.agilelab.bigdata.wasp.core.launcher

import akka.actor.Props

/**
	* Launcher for a single cluster singleton actor.
	*
	* @author Nicolò Bidotti
	*/
trait ClusterSingletonLauncher extends MultipleClusterSingletonsLauncher {
	override final def getSingletonInfos: Seq[(Props, String, String, Seq[String])] = {
		Seq((getSingletonProps, getSingletonName, getSingletonManagerName, getSingletonRoles))
	}
	
	/**
		* Props for the singleton actor.
		*/
	def getSingletonProps: Props
	
	/**
		* Name for the singleton actor.
		*/
	def getSingletonName: String
	
	/**
		* Name for the singleton manager actor.
		*/
	def getSingletonManagerName: String
	
	/**
		* Roles for this singleton.
		*/
	def getSingletonRoles: Seq[String]
}
