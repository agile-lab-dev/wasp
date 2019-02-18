package it.agilelab.bigdata.wasp.core.eventengine.eventproducers

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.core.eventengine.settings.{EventPipegraphSettings, EventPipegraphSettingsFactory, EventProducerETLSettings}
import it.agilelab.bigdata.wasp.core.eventengine.{EventReaderModelFactory, EventTopicModelFactory}
import it.agilelab.bigdata.wasp.core.models._

/**
  * EventPipegraph is a System pipegraph which produces Event objects.
  * An Event object is the result of some sort of trigger applied on an input data streaming source.
  *
  * In order to activate the event production, the user has to properly define the event-pipegraph configuration
  * properties in the event-engine section .conf files. EventPipegraph configuration is in turn composed by a
  * list of ETL configuration objects, and the user can generate an arbitrary number of event streaming
  * flow by defining as many ETL configuration objects.
  *
  * For each described event streaming flow, it will be spawned a dedicated ETL. Event production ETLs are composed by:
  *    *   A single source of data from a Kafka topic usually domain-specific data, in any data model
  *    *   A single sink on a Kafka topic to store Event objects, which are in a specific data model
  *    *   A set of rules which trigger the creation of an Event object.
  *
  * Different ETLs can have the same source of data, as well as different ETLs can store data on the same sink.
  *
  *     +--------+   +--------+                +--------+
  *     |  Data  |   |  Data  |                |  Data  |
  *     | Source |   | Source |                | Source |
  *     +---+----+   +---+-+--+                +---+----+
  *         |            | |----------+            |
  *         |            |            |            |
  *     +---v----+   +---v----+   +---v----+   +---v----+
  *     | Event  |   | Event  |   | Event  |   | Event  |
  *     |Producer|   |Producer|   |Producer|   |Producer|
  *     |  ETL   |   |  ETL   |   |  ETL   |   |  ETL   |
  *     +---+----+   +---+----+   +----+---+   +----+---+
  *         |            |             |            |
  *         |            |             +----------| |
  *     +---v----+   +---v----+                +--v-v---+
  *     | Event  |   | Event  |                | Event  |
  *     | Sink   |   | Sink   |                | Sink   |
  *     +--------+   +--------+                +--------+
  *
  * Update: EventPipegraphModel read a isSystem flag from configuration which declares whether or not the Pipegraph should
  * be automatically started when starting Wasp with the startSystemPipegraph option. The default value in case the isSystem
  * keyword is not present is false
  *
  */

object EventPipegraphModel {

  private val eventPipegraphSettings: EventPipegraphSettings = EventPipegraphSettingsFactory.create(ConfigFactory.load())
  private val isSystem: Boolean = eventPipegraphSettings.isSystem
  private val eventEngineSettings: Seq[EventProducerETLSettings] = eventPipegraphSettings.eventStrategies

  private val tuples: List[(TopicModel, StructuredStreamingETLModel)] =
    eventEngineSettings.map(s => {

      val outputTopicModel = EventTopicModelFactory.create(s.writerModel)

      val etlModel = StructuredStreamingETLModel(
        name = s.name, // Maybe streaming source here?
        // Defines the endpoint and the source type
        streamingInput = EventReaderModelFactory.create(s.readerModel),
        staticInputs = List.empty,
        // Defines the endpoint and the sink type
        streamingOutput = WriterModel.kafkaWriter(s.writerModel.dataStoreModelName, outputTopicModel),
        mlModels = List.empty,
        // Defines what to do with the data retrieved from the source
        strategy = Some(StrategyModel.create("it.agilelab.bigdata.wasp.consumers.spark.eventengine.EventStrategy", s.trigger)),
        triggerIntervalMs = if(s.triggerIntervalMs.isDefined) s.triggerIntervalMs else eventPipegraphSettings.defaultTriggerIntervalMs
      )


      (outputTopicModel, etlModel)
    }).toList

  //Expose both topic models and etl models
  val (outputTopicModels, eventETLModels) = (tuples.map(_._1), tuples.map(_._2))

  lazy val eventPipegraph = PipegraphModel (
    name = "EventPipegraph",
    description = "This Pipegraph produces Events",
    owner = "user",
    isSystem = isSystem,
    creationTime = System.currentTimeMillis,
    legacyStreamingComponents = List.empty,
    structuredStreamingComponents = eventETLModels,
    rtComponents = List.empty,
    dashboard = None)

}
