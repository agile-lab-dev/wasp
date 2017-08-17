package it.agilelab.bigdata.wasp.core.logging

/**
	* Helper trait for logging support backed by a WaspLogger instance.
	*
	* @author Nicolò Bidotti
	*/
trait Logging {
	protected val logger = WaspLogger(this.getClass)
}
