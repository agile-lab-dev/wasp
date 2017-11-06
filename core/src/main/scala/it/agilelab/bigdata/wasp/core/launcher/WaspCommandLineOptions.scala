package it.agilelab.bigdata.wasp.core.launcher

import org.apache.commons.cli.{Option => CliOption}


object WaspCommandLineOptions {
	def version: CliOption = new CliOption("v", "version", false, "Print version and exit")
	def help: CliOption = new CliOption("h", "help", false, "Print help and exit")
	
	val requiredOptions = Seq.empty[CliOption]
	val otherOptions = Seq(
		version,
		help
	)
	
	def allOptions: Seq[CliOption] = requiredOptions ++ otherOptions
}
