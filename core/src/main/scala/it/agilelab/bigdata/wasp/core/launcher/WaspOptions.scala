package it.agilelab.bigdata.wasp.core.launcher

class WaspOptions(args: Seq[String]) {
	var version = false
	var help = false

	var startApp = false
	// var startMasterGuardian = false
	var startStreamingGuardian = false
	var startBatchGuardian = false

	var error = false
	var errorValue: String = _

	// set options by parsing arguments
	val argsList = args.toList
	if (argsList.size > 0 ) parseOpts(argsList) else startApp = true // default is start the web app

	// parse command line options by recursively slicing the seq
	private def parseOpts(opts: Seq[String]): Unit =  opts match {
		case ("--version" | "-v") :: tail =>
			version = true
			parseOpts(tail)

		case ("--help" | "-h") :: tail =>
			help = true
			parseOpts(tail)

		case ("--app") :: tail =>
			startApp = true
			parseOpts(tail)
		/*
		case ("--master") :: tail =>
			startMaster = true
			parseOpts(tail)
		*/
		case ("--streaming-guardian" | "-s") :: tail =>
			startStreamingGuardian = true
			parseOpts(tail)

		case ("--batch-guardian" | "-b") :: tail =>
			startBatchGuardian = true
			parseOpts(tail)

		case value :: tail =>
			error = true
			errorValue = value

		case Nil => // end recursion
	}
}
