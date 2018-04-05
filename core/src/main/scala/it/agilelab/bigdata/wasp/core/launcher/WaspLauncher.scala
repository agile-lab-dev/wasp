package it.agilelab.bigdata.wasp.core.launcher

import java.util.concurrent.TimeUnit

import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.WaspSystem.waspConfig
import it.agilelab.bigdata.wasp.core.build.BuildInfo
import it.agilelab.bigdata.wasp.core.models.configuration.{ValidationRule, WaspConfigModel}
import it.agilelab.bigdata.wasp.core.utils.{CliUtils, ConfigManager, MongoDBHelper, WaspDB}
import org.apache.commons.cli
import org.apache.commons.cli.{CommandLine, ParseException}

import scala.concurrent.Await
import scala.concurrent.duration.Duration

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
				initializeWasp(commandLine)

				// initialize plugins - really defined only for spark consumers nodes (streaming and batch)
				initializePlugins(args)

				// validate configs - use of plugin-level validationRules only for spark consumers nodes (streaming and batch), which define them
				validateConfigs()

				// launch the application
				launch(commandLine)

			} catch {
				case e: ParseException => // error parsing cli args (already printed usage) => print error and exit
					printErrorAndExit(e.getMessage)
				case e: Exception => // generic WASP error
					throw e
			}
	}

	def initializeWasp(commandLine: CommandLine): Unit = {

		waspDB = WaspDB.initializeDB()

		/* Management of dropDB commandline ption */
		if(getNodeName == "master") {
			if (commandLine.hasOption(MasterCommandLineOptions.dropDb.getOpt)) {
				// drop db
				val mongoDBConfig = ConfigManager.getMongoDBConfig

				println(s"Dropping MongoDB database '${mongoDBConfig.databaseName}'")
				val mongoDBDatabase = MongoDBHelper.getDatabase(mongoDBConfig)
				val dropFuture = mongoDBDatabase.drop().toFuture()
				Await.result(dropFuture, Duration(10, TimeUnit.SECONDS))
				println(s"Dropped MongoDB database '${mongoDBConfig.databaseName}'")
				System.exit(0)

				// re-initialize mongoDB and continue (instead of exit) -> not safe due to all process could write on mongoDB
				//waspDB = WaspDB.initializeDB()
			}
		}

		ConfigManager.initializeCommonConfigs()

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
		* Initialize the WASP plugins, this method is called after the WASP initialization.
		*
		* Default: no nothing.
		* Overrided by spark consumers nodes (streaming and batch): plugin initialization
		*
		* @param args command line arguments
		*/
	def initializePlugins(args: Array[String]): Unit = Unit

	/**
		* Validate the configs, this methos is called after the WASP plugin initializations
		*
		* Default: use global-level validationRules in [[ConfigManager]].
		* Overrided by spark consumers nodes (streaming and batch): use global-level and plugin-level validationRules defined only for spark consumers nodes (streaming and batch)
		*
		* @param pluginsValidationRules
		*/
	def validateConfigs(pluginsValidationRules: Seq[ValidationRule] = Seq()): Unit = {
		println("Configs validation")

		// validate configs
		val validationResults = ConfigManager.validateConfigs(pluginsValidationRules)

		println(s"VALIDATION-RESULT:\n\t${
			validationResults
				.map( pair => pair._1 -> (if(pair._2.isLeft) "NOT PASSED" else "PASSED"))
				.mkString("\n\t")
		}")

		if (validationResults.exists(_._2.isLeft)) {
			// there is at least a validation failure

			if(waspConfig.environmentMode == WaspConfigModel.WaspEnvironmentMode.develop)
				println(s"VALIDATION-WARN: Configs NOT successfully validated. Continuation due to WASP is launched in 'develop' mode")
			else {
				// all not "develop" is considered "production" by default
				println(s"VALIDATION-ERROR: Configs NOT successfully validated. Termination due to WASP is launched in 'production' mode (environment.mode = '${waspConfig.environmentMode}')")
				System.exit(1)

				// TODO stop all cluster nodes due to pluginsValidationRules are not validated by all nodes (i.e. a failures related to pluginsValidationRules are not recognized by all nodes)
			}
		}
		else
			println("Configs successfully validated")
	}

	def getNodeName: String
}