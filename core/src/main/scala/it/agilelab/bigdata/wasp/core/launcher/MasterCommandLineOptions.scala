package it.agilelab.bigdata.wasp.core.launcher

import org.apache.commons.cli.{Option => CliOption}


/**
	* @author Nicolò Bidotti
	*/
object MasterCommandLineOptions {
	def dropDb: CliOption = new CliOption("d", "drop-db", false, "Drop MongoDB database")
	
	def allOptions: Seq[CliOption] = Seq(dropDb)
}
