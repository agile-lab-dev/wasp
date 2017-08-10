package it.agilelab.bigdata.wasp.core.models.configuration

/**
	* Configuration model for WASP.
	*
	* @author Nicolò Bidotti
	*/
case class WaspConfigModel(
	                         actorSystemName: String,
                           indexRollover: Boolean,
                           generalTimeoutMillis: Int,
                           servicesTimeoutMillis: Int,
                           additionalJarsPath: String,
                           defaultIndexedDatastore: String,
                           defaultKeyvalueDatastore: String
                          )
