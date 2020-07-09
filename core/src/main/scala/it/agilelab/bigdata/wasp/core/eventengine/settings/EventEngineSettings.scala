package it.agilelab.bigdata.wasp.core.eventengine.settings

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.core.eventengine.eventconsumers.MailingRule
import it.agilelab.bigdata.wasp.core.eventengine.eventproducers.EventRule

/**
  * Models supported by the event engine to load and store events
  * Currently only Kafka and mail are supported
 */
object ModelTypes extends Enumeration {
  val KAFKA, MAIL = Value
}

case class EventPipegraphSettings(eventStrategies: Seq[EventProducerETLSettings],
                                 defaultTriggerIntervalMs: Option[Long],
                                 isSystem: Boolean)

case class MailingPipegraphSettings(mailWriterSettings: Option[ModelSettings],
                                    defaultTriggerIntervalMs: Option[Long],
                                    mailingStrategies: Seq[EventConsumerETLSettings], isSystem: Boolean)

/**
  * Event producer ETL settings defines the Event generation flow.
  * An [[EventProducerETLSettings]] object refers to a single [[StructuredStreamingETLModel]].
  *
  * To give more freedom to users, different event producers can fetch operational data from the same source, as well as
  * write the found Event objects on the same sink
  *
  * @param name is the name of the EventStrategy to create
  * @param readerModel is the information about where to fetch operational data
  * @param writerModel is the information about where to store computed Event objects
  * @param trigger is the Typesafe config object which defines event rules
  */
case class EventProducerETLSettings(name: String,
                                    triggerIntervalMs: Option[Long],
                                    readerModel: ModelSettings,
                                    writerModel: ModelSettings,
                                    trigger: Config)

/**
  * Event consumer ETL settings defines the Event consumption flow.
  * A [[EventConsumerETLSettings]] objects refers to a single [[StructuredStreamingETLModel]].
  *
  * @param name is the name of the MailingStrategy to create
  * @param readerModel is the information about where to fetch Event objects
  * No writerModel because mail pipegraph is a funnel, so it's defined at pipegraph configuration level
  * @param trigger is the Typesafe config object which defines mail rules
  */
case class EventConsumerETLSettings(name: String,
                                    triggerIntervalMs: Option[Long],
                                    readerModel: ModelSettings,
                                    trigger: Config)

/**
  * A class to store information about a [[TopicModel]]
  * @param modelName is the identifier of the model
  * @param dataStoreModelName is the name of the dataStore model (ATM kafka topic)
  * @param modelType is the type of the model among the supported ones
  */
case class ModelSettings(modelName: String,
                         dataStoreModelName: String,
                         modelType: ModelTypes.Value,
                         options: Map[String, String] = Map.empty)

/**
  * Defines settings for a single EventStrategy
  * @param rules is the sequence of EventRule
  */
case class EventStrategySettings(rules: Seq[EventRule])

/**
  * Defines settings for a single MailingStrategy
  * @param rules is the sequence of MailingRule
  * @param enableMailAggregation: if true mails with same recipient will be aggregated in a single Mail
  */
case class MailingStrategySettings(rules: Seq[MailingRule], enableMailAggregation: Boolean = false)