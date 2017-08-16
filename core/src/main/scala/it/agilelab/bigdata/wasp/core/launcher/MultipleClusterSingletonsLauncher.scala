package it.agilelab.bigdata.wasp.core.launcher

import akka.actor.{PoisonPill, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import it.agilelab.bigdata.wasp.core.WaspSystem

/**
	* Launcher for multiple cluster singleton actors.
	*
	* @author Nicolò Bidotti
	*/
trait MultipleClusterSingletonsLauncher extends WaspLauncher {
	override def launch(args: Array[String]): Unit = {
		val actorSystem = WaspSystem.actorSystem
		
		// helper for adding role to ClusterSingletonManagerSettings
		val addRoleToSettings = (settings: ClusterSingletonManagerSettings, role: String) => settings.withRole(role)
		
		// for each singleton to launch
		getSingletonInfos foreach {
			case (props, name, roles) => {
				// build the settings
				val settings = roles.foldLeft(ClusterSingletonManagerSettings(actorSystem))(addRoleToSettings)
				// spawn the cluster singleton manager, which will spawn the actual singleton actor as defined by the getters
				actorSystem.actorOf(
					ClusterSingletonManager.props(
						singletonProps = props,
						terminationMessage = PoisonPill,
						settings = settings
					),
					name = name
				)
			}
		}
	}
	
	/**
		* Get the list of triples describing the cluster singletons to start.
		*
		* The triples' elements are:
		* - actor's Props
		* - actor's name
		* - valid node roles
		* @return
		*/
	def getSingletonInfos: Seq[(Props, String, Seq[String])]
}