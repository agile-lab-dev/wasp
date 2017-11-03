package it.agilelab.bigdata.wasp.core.launcher

import org.apache.commons.cli.{Option => CliOption}


object WaspOptions {
	
	def version: CliOption = new CliOption("v", "version", true, "Print version and exit")
	
	def help: CliOption = new CliOption("h", "help", false, "Print help and exit")
	
	val requiredOptions = Seq.empty[CliOption]
	
	val otherOptions = Seq(
		version,
		help
	)
	
	def allOptions: Seq[CliOption] = requiredOptions ++ otherOptions
}
