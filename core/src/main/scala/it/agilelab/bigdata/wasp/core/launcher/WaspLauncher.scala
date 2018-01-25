package it.agilelab.bigdata.wasp.core.launcher

import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.build.BuildInfo
import it.agilelab.bigdata.wasp.core.utils.{CliUtils, ConfigManager, WaspDB}
import org.apache.commons.cli
import org.apache.commons.cli.CommandLine


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
	}

	def initializeWasp(): Unit = {
		// db
		WaspDB.initializeDB()
		waspDB = WaspDB.getDB
		// configs
		ConfigManager.initializeCommonConfigs()
		// "special" configs (if needed)
		initializeConfigurations()
		// waspsystem
		WaspSystem.initializeSystem()
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

	def initializeConfigurations(): Unit = {
		Unit
	}

	def getNodeName: String
}