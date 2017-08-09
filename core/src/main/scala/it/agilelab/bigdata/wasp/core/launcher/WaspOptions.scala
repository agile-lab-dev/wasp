package it.agilelab.bigdata.wasp.core.launcher

// TODO switch to commons-cli & make options extensible
class WaspOptions(args: Seq[String]) {
	var version = false
	var help = false

	var error = false
	var errorValue: String = _

	// set options by parsing arguments
	val argsList = args.toList
	parseOpts(argsList)

	// parse command line options by recursively slicing the seq
	private def parseOpts(opts: Seq[String]): Unit =  opts match {
		case ("--version" | "-v") :: tail =>
			version = true
			parseOpts(tail)

		case ("--help" | "-h") :: tail =>
			help = true
			parseOpts(tail)

		case value :: tail =>
			error = true
			errorValue = value

		case Nil => // end recursion
	}
}
