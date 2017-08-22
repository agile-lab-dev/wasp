package it.agilelab.bigdata.wasp.core.launcher

import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.build.BuildInfo
import it.agilelab.bigdata.wasp.core.utils.{ConfigManager, WaspDB}


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

	// TODO write usage information (when command line switches are somewhat definitive)
	val usage: String = """Usage:
			TODO!
		          """.stripMargin

	var waspDB = _

	def main(args: Array[String]) {
		// TODO switch to commons-cli & make options extensible
		val options = new WaspOptions(args)

		// handle error, version & help
		if (options.error) {
			val value = options.errorValue
			printErrorAndExit(s"Unrecognized option '$value'.")
		} else if (options.version) {
			printVersionAndExit()
		} else if (options.help) {
			printUsageAndExit()
		}

		// print banner and build info
		printBannerAndBuildInfo()

		// initialize stuff
		initializeWasp()
		
		// launch the application
		launch(args)
	}

	def initializeWasp(): Unit = {
		// db
		WaspDB.initializeDB()
		waspDB = WaspDB.getDB
		// configs
		ConfigManager.initializeConfigs()
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

	private def printUsageAndExit(): Unit = {
		println(usage)
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
	
	def launch(args: Array[String]): Unit
	
	def getNodeName: String
}