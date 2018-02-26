package it.agilelab.bigdata.wasp.core.launcher

import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.actorSystem
import it.agilelab.bigdata.wasp.core.build.BuildInfo
import it.agilelab.bigdata.wasp.core.cluster.ClusterListenerActor
import it.agilelab.bigdata.wasp.core.messages.DownUnreachableMembers
import it.agilelab.bigdata.wasp.core.utils.{CliUtils, ConfigManager, WaspDB}
import org.apache.commons.cli
import org.apache.commons.cli.{CommandLine, ParseException}

trait WaspLauncher {
	// the actual version of WASP being ran
	val version: String = BuildInfo.version // BuildInfo is generated at compile time by sbt-buildinfo plugin
	
	// ASCII art from http://bigtext.org/?font=smslant&text=Wasp
	val banner: String = """Welcome to
     _       __
    | |     / /___ _ ____ ___
    | | /| / / __ `/ ___/ __ \
    | |/ |/ / /_/ (__  ) /_/ /
    |__/|__/\__,_/____/ .___/    version %s
                     /_/
								               """.format(version)

	var waspDB: WaspDB = _

	def main(args: Array[String]) {

			try {
				// parse command line
				val commandLine = CliUtils.parseArgsList(args, getOptions)

				// handle version & help
				if (commandLine.hasOption(WaspCommandLineOptions.version.getOpt)) {
					printVersionAndExit()
				} else if (commandLine.hasOption(WaspCommandLineOptions.help.getOpt)) {
					printHelpAndExit()
				}

				// print banner and build info
				printBannerAndBuildInfo()

				// initialize stuff
				initializeWasp()

				// initialize plugins
				initializePlugins(args)

				// launch the application
				launch(commandLine)

			} catch {
				case e: ParseException => // error parsing cli args (already printed usage) => print error and exit
					printErrorAndExit(e.getMessage)
				case e: Exception => // generic WASP error
					throw e
			}
	}

	def initializeWasp(): Unit = {
		// db
		WaspDB.initializeDB()
		waspDB = WaspDB.getDB
		// configs
		ConfigManager.initializeCommonConfigs()

		// waspsystem
		WaspSystem.initializeSystem()

		/* Only for Debug: print Akka actor system tree
		implicit val dispatcher = actorSystem.dispatcher
		import scala.concurrent.duration._
		actorSystem.scheduler.scheduleOnce(100 seconds)  {
			val res = new PrivateMethodExposer(actorSystem)('printTree)()
			println(res)
		}


		class PrivateMethodCaller(x: AnyRef, methodName: String) {
			def apply(_args: Any*): Any = {
				val args = _args.map(_.asInstanceOf[AnyRef])

				def _parents: Stream[Class[_]] = Stream(x.getClass) #::: _parents.map(_.getSuperclass)

				val parents = _parents.takeWhile(_ != null).toList
				val methods = parents.flatMap(_.getDeclaredMethods)
				val method = methods.find(_.getName == methodName).getOrElse(throw new IllegalArgumentException("Method " + methodName + " not found"))
				method.setAccessible(true)
				method.invoke(x, args: _*)
			}
		}

		class PrivateMethodExposer(x: AnyRef) {
			def apply(method: scala.Symbol): PrivateMethodCaller = new PrivateMethodCaller(x, method.name)
		}
		*/
	}
	
	private def printErrorAndExit(message: String): Unit = {
		println(message)
		println("Use --help for usage information.")
		System.exit(0)
	}

	private def printVersionAndExit(): Unit = {
		println(s"WASP version $version")
		System.exit(0)
	}

	private def printHelpAndExit(): Unit = {
		CliUtils.printHelpForOptions(getOptions)
		System.exit(0)
	}
	
	private def printBannerAndBuildInfo(): Unit = {
		println(banner)
		println(
			s"""Build information:
				 |  Version         : ${BuildInfo.version}
				 |  SBT version     : ${BuildInfo.sbtVersion}
				 |  Scala version   : ${BuildInfo.scalaVersion}
				 |  JDK version     : ${BuildInfo.jdkVersion}
				 |  Build time      : ${BuildInfo.builtAtString} (UNIX time)
				 |  Git commit hash : ${BuildInfo.gitCommitHash}
				 |  Git work dir    : ${if (BuildInfo.gitWorkDirStatus) "clean" else "dirty"}
			 """.stripMargin)
		println(s"This is WASP node $getNodeName")
	}
	
	protected def launch(commadLine: CommandLine): Unit
	
	protected def getOptions: Seq[cli.Option] = WaspCommandLineOptions.allOptions

	/**
		* Initialize the WASP plugins, this method is called after the wasp initialization
		* @param args command line arguments
		*/
	def initializePlugins(args: Array[String]): Unit = {
		Unit
	}

	def getNodeName: String
}