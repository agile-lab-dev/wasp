package it.agilelab.bigdata.wasp.core.eventengine.eventconsumers

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.core.eventengine.EventReaderModelFactory
import it.agilelab.bigdata.wasp.core.eventengine.settings.{MailingPipegraphSettings, MailingPipegraphSettingsFactory}
import it.agilelab.bigdata.wasp.models.configuration.RestEnrichmentConfigModel
import it.agilelab.bigdata.wasp.models.{PipegraphModel, StrategyModel, StructuredStreamingETLModel, WebMailModel, WriterModel}

/**
  * MailingPipegraph is a System pipegraph which consumes Event objects.
  *
  * In order to activate the event consumption, the user has to properly define the mailing-pipegraph configuration
  * properties in the event-engine section .conf files. MailingPipegraph configuration is composed by a MailWriter
  * definition and a list of ETL configuration objects. The user can generate an arbitrary number of event mailing
  * flow by defining as many configuration objects.
  *
  * The MailWriter configuration stores the information required to access the smtp-server with the given account.
  *
  * For each described event mailing flow, it will be spawned a dedicated ETL. Event consumption ETLs are composed by:
  *    *   A single source of Event objects from a Kafka topic
  *    *   A set of rules which trigger the creation of an Event object.
  *
  *
  *      +--------+   +--------+                +--------+
  *      | Event  |   | Event  |                | Event  |
  *      | Source |   | Source |                | Source |
  *      +---+----+   +---+-+--+                +--------+
  *          |            | |----------+            |
  *          |            |            |            |
  *      +---v----+   +---v----+   +---v----+   +---v----+
  *      | Event  |   | Event  |   | Event  |   | Event  |
  *      |Consumer|   |Consumer|   |Consumer|   |Consumer|
  *      |  ETL   |   |  ETL   |   |  ETL   |   |  ETL   |
  *      +--------+   +--------+   +--------+   +--------+
  *          |              |        |               |
  *          |              |        |               |
  *          |            +-v--------v-+             |
  *          |            |   Mailer   |             |
  *          +----------- >    Sink    <-------------+
  *                       +------------+
  *
  * Different ETLs can have the same source of data, but all ETLs will have the same Mailer Sink.
  *
  *
  * Update: MailingPipegraphModel read a isSystem flag from configuration which declares whether or not the Pipegraph should
  * be automatically started when starting Wasp with the startSystemPipegraph option. The default value in case the isSystem
  * keyword is not present is false
  */
object MailingPipegraphModel {

  //TODO: evaluate injectable configuration
  private val mailingPipegraphSettings: MailingPipegraphSettings = MailingPipegraphSettingsFactory.create(ConfigFactory.load())
  private val isSystem: Boolean = mailingPipegraphSettings.isSystem
  private val maybeWriterSettings = mailingPipegraphSettings.mailWriterSettings
  private val strategiesSettings = mailingPipegraphSettings.mailingStrategies

  private val sanitizedOptions: Map[String, String] = maybeWriterSettings match {
    case Some(writerSettings) => writerSettings.options.map( e => (e._1.replace(".", "___"), e._2) )
    case None => Map()
  }



  // Send events ad mail
  private val mailerWriterModel: WriterModel = maybeWriterSettings match {
    case Some(writerSettings) => WriterModel.webMailWriter(writerSettings.modelName, WebMailModel(writerSettings.dataStoreModelName), sanitizedOptions)
    case None => WriterModel.consoleWriter("Unused")
  }


  val mailingETLModels: List[StructuredStreamingETLModel] =
    strategiesSettings.map(s => { StructuredStreamingETLModel (
        name = s.name,
        // Defines the endpoint and the source type
        streamingInput = EventReaderModelFactory.create(s.readerModel),
        staticInputs = List.empty,
        // Defines the endpoint and the sink type
        streamingOutput = mailerWriterModel,
        mlModels = List.empty,
        // Defines what to do with the data retrieved from the source
        strategy = Some(StrategyModel.create("it.agilelab.bigdata.wasp.consumers.spark.eventengine.MailStrategy", s.trigger)),
        triggerIntervalMs = if(s.triggerIntervalMs.isDefined) s.triggerIntervalMs else mailingPipegraphSettings.defaultTriggerIntervalMs
      )
    }).toList


  lazy val mailingPipegraph = PipegraphModel (
    name = "MailingPipegraph",
    description = "This Pipegraph consumes Events",
    owner = "user",
    isSystem = isSystem,
    creationTime = System.currentTimeMillis,
    legacyStreamingComponents = List.empty,
    structuredStreamingComponents = mailingETLModels,
    rtComponents = List.empty,
    dashboard = None,
    enrichmentSources = RestEnrichmentConfigModel(Map.empty))

}
