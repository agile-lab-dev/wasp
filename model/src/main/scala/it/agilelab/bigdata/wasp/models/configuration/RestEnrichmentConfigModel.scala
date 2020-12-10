package it.agilelab.bigdata.wasp.models.configuration

case class RestEnrichmentConfigModel(sources: Map[String, RestEnrichmentSource])

case class RestEnrichmentSource(
                                 kind: String,
                                 parameters: Map[String, String],
                                 headers: Map[String, String] = Map.empty
                               )