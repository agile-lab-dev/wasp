package it.agilelab.bigdata.wasp.models.configuration

import scala.util.matching.Regex

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
    systemPipegraphsStart: Boolean,
    systemProducersStart: Boolean,
    restServerHostname: String,
    restServerPort: Int,
    restHttpsConf: Option[RestHttpsConfigModel],
    environmentPrefix: String,
    validationRulesToIgnore: Seq[String],
    environmentMode: String,
    darwinConnector: String,
    configurationMode: ConfigurationMode
)

sealed trait ConfigurationMode

object ConfigurationMode {

  val namespaceRegexp: Regex = """^namespaced://(.*)$""".r("namespace")

  def asString(configurationMode: ConfigurationMode): String = configurationMode match {
    case Legacy                              => "legacy"
    case Local                               => "local"
    case NamespacedConfigurations(namespace) => s"namespaced://$namespace"
  }

  def fromString(configurationMode: String): ConfigurationMode = configurationMode match {
    case "legacy"                   => Legacy
    case "local"                    => Local
    case namespaceRegexp(namespace) => NamespacedConfigurations(namespace)
    case _ =>
      throw new IllegalArgumentException(
        s"Provided configuration mode [$configurationMode] is not recognized," +
          s" available values are [legacy, local, namespaced://namespacename"
      )
  }

  case object Legacy                                     extends ConfigurationMode
  case object Local                                      extends ConfigurationMode
  case class NamespacedConfigurations(namespace: String) extends ConfigurationMode
}

object WaspConfigModel {
  object WaspEnvironmentMode {
    val develop = "develop"
  }
}

case class RestHttpsConfigModel(
    keystoreLocation: String,
    passwordLocation: String,
    keystoreType: String
)
