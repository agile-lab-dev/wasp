package it.agilelab.bigdata.wasp.core.models.configuration

/**
	* Configuration model for WASP.
	*
	*/
case class WaspConfigModel(
	                          actorSystemName: String,
														actorDowningTimeout: Int,
	                          indexRollover: Boolean,
	                          generalTimeoutMillis: Int,
	                          servicesTimeoutMillis: Int,
	                          defaultIndexedDatastore: String,
	                          defaultKeyvalueDatastore: String,
	                          systemPipegraphsStart: Boolean,
	                          systemProducersStart: Boolean,
	                          restServerHostname: String,
	                          restServerPort: Int,
														environmentPrefix: String //This should not contain space or /. chars
                          )