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
														environmentPrefix: String,
														validationRulesToIgnore: Seq[String],
														environmentMode: String
                          )

object WaspConfigModel {
	object WaspEnvironmentMode {
		val develop = "develop"
	}
}