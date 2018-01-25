package it.agilelab.bigdata.wasp.core.launcher

import akka.actor.{PoisonPill, Props}
import akka.cluster.singleton.{ClusterSingletonManager, ClusterSingletonManagerSettings}
import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.logging.Logging
import org.apache.commons.cli.CommandLine

/**
	* Launcher for multiple cluster singleton actors.
	*
	* @author Nicolò Bidotti
	*/
trait MultipleClusterSingletonsLauncher extends WaspLauncher with Logging {
	override def launch(commandLine: CommandLine): Unit = {
		val actorSystem = WaspSystem.actorSystem
		
		// helper for adding role to ClusterSingletonManagerSettings
		val addRoleToSettings = (settings: ClusterSingletonManagerSettings, role: String) => settings.withRole(role)
		
		// for each singleton to launch
		getSingletonInfos foreach {
			case (singletonProps, singletonName, singletonManagerName, roles) => {
				logger.info(s"""Starting cluster singleton manager "$singletonManagerName" """ +
					            s"""for singleton "$singletonName" """ +
					            s"with props $singletonProps " +
					            s"and roles $roles")
				
				// build the settings
				val settings = roles.foldLeft(ClusterSingletonManagerSettings(actorSystem))(addRoleToSettings)
				
				// spawn the cluster singleton manager, which will spawn the actual singleton actor as defined by the getters
				val singletonManager = actorSystem.actorOf(
					ClusterSingletonManager.props(
						singletonProps = singletonProps,
						terminationMessage = PoisonPill,
						settings = settings.withSingletonName(singletonName)
					),
					name = singletonManagerName
				)
				
				logger.info(s"Started singleton manager $singletonManager")
				
				singletonManager
			}
		}
	}
	
	/**
		* Get the list of quadruples describing the cluster singletons to start.
		*
		* The triples' elements are:
		* - singleton actor's Props
		* - singleton actor's name
		* - singleton manager actor's name
		* - valid node roles
		* @return
		*/
	def getSingletonInfos: Seq[(Props, String, String, Seq[String])]
}
