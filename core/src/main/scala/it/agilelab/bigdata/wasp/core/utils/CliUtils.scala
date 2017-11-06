package it.agilelab.bigdata.wasp.core.utils

import java.io.PrintWriter

import org.apache.commons.cli
import org.apache.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}

import scala.util.{Failure, Success, Try}

/**
	* Command line utils leveraging Apache Commons CLI
	*/
object CliUtils {
	def parseArgsList(args: Array[String],
	                  options: Seq[cli.Option]): CommandLine = {
		val cliOptions = seqToCliOptions(options)
		
		Try {
			    new DefaultParser().parse(cliOptions, args)
		    } match {
			case Success(settings) => {
				settings
			}
			case Failure(e) => printHelpForOptions(options); throw e
		}
	}
	
	def printHelpForOptions(options:  Seq[cli.Option]) {
		val cliOptions = seqToCliOptions(options)
		val sysOut = new PrintWriter(System.out)
		new HelpFormatter().printUsage(sysOut, 100, "WASP", cliOptions)
		sysOut.close()
	}
	
	private def seqToCliOptions(options: Seq[cli.Option]) = options.foldLeft(new Options)( _ addOption _ )
}