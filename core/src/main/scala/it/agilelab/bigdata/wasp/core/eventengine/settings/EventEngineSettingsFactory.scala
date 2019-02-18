package it.agilelab.bigdata.wasp.core.eventengine.settings

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.core.eventengine.eventconsumers.MailingRule
import it.agilelab.bigdata.wasp.core.eventengine.eventproducers.EventRule
import it.agilelab.bigdata.wasp.core.eventengine.EventEngineConstants._

import scala.collection.JavaConverters._

object EventPipegraphSettingsFactory {
  private lazy val keyword = "wasp.eventengine.eventPipegraph"
  private lazy val prefix =  keyword + "."

  def create(config: Config): EventPipegraphSettings = {

    //If you don't want to use eventPipegraph, simply do not include the keyword
    if(config.hasPath(keyword)) {
      val isSystem = if(config.hasPath(prefix + IS_SYSTEM)) config.getBoolean(prefix + IS_SYSTEM) else false

      val defaultTriggerIntervalMs: Option[Long] =
        if (config.hasPath(prefix + TRIGGER_INTERVAL_MS)) Some(config.getInt(prefix + TRIGGER_INTERVAL_MS)) else None


      val eventStrategyList: Seq[EventProducerETLSettings] =
        config.getConfigList(prefix + EVENT_STRATEGY).asScala.map(cfg => {
          val name = cfg.getString(NAME)

          val triggerIntervalMs: Option[Long] =
            if (config.hasPath(prefix + TRIGGER_INTERVAL_MS)) Some(config.getInt(prefix + TRIGGER_INTERVAL_MS)) else None

          val readerModel = ModelSettingsFactory.create(cfg.getConfig(READER))
          val writerModel = ModelSettingsFactory.create(cfg.getConfig(WRITER))

          val triggerCfg = cfg.getConfig(TRIGGER)

          EventProducerETLSettings(
            name = name,
            triggerIntervalMs = triggerIntervalMs,
            readerModel = readerModel,
            writerModel = writerModel,
            trigger = triggerCfg)
        })

      EventPipegraphSettings(
        eventStrategies = eventStrategyList,
        defaultTriggerIntervalMs = defaultTriggerIntervalMs,
        isSystem = isSystem)
    } else {
      EventPipegraphSettings(
        eventStrategies = Seq(),
        defaultTriggerIntervalMs = None,
        isSystem = false)
    }


  }

}

object MailingPipegraphSettingsFactory {
  private lazy val keyword = "wasp.eventengine.mailingPipegraph"
  private lazy val prefix = keyword + "."

  def create(config: Config): MailingPipegraphSettings = {

    //If you don't want to use mailingPipegraph, simply do not include the keyword
    if(config.hasPath(keyword)){
      val isSystem = if(config.hasPath(prefix + IS_SYSTEM)) config.getBoolean(prefix + IS_SYSTEM) else false

      val defaultTriggerIntervalMs: Option[Long] =
        if (config.hasPath(prefix + TRIGGER_INTERVAL_MS)) Some(config.getInt(prefix + TRIGGER_INTERVAL_MS)) else None

      val writerModel = ModelSettingsFactory.create(config.getConfig(prefix + WRITER))

      val mailingStrategyList: Seq[EventConsumerETLSettings] =
        config.getConfigList(prefix + MAILING_STRATEGY).asScala.map(cfg => {
          val name = cfg.getString(NAME)

          val triggerIntervalMs: Option[Long] =
            if (config.hasPath(prefix + TRIGGER_INTERVAL_MS)) Some(config.getInt(prefix + TRIGGER_INTERVAL_MS)) else None

          val readerModel = ModelSettingsFactory.create(cfg.getConfig(READER))

          val triggerCfg = cfg.getConfig(TRIGGER)

          EventConsumerETLSettings(
            name = name,
            triggerIntervalMs = triggerIntervalMs,
            readerModel = readerModel,
            trigger = triggerCfg)
        })

      MailingPipegraphSettings(
        mailWriterSettings = Some(writerModel),
        mailingStrategies = mailingStrategyList,
        defaultTriggerIntervalMs = defaultTriggerIntervalMs,
        isSystem = isSystem)
    } else {

      MailingPipegraphSettings(
        mailWriterSettings = None,
        mailingStrategies = Seq(),
        defaultTriggerIntervalMs = None,
        isSystem = false)
    }
  }

}

object ModelSettingsFactory {

  def create(config: Config): ModelSettings = {

    val modelType = config.getString(MODEL_TYPE)
    val modelName = config.getString(MODEL_NAME)
    val dataStoreModelName = config.getString(DATASTORE_MODEL_NAME)

    val options = {
      if (config.hasPath(OPTIONS)) {
        val optCfg = config.getObjectList(OPTIONS)

        optCfg.asScala.map(e => {
          val cfg = e.toConfig
          cfg.getString(KEY) -> cfg.getString(VALUE)
        }).toMap
      } else Map[String, String]()
    }

    ModelSettings(
      modelName = modelName,
      dataStoreModelName = dataStoreModelName,
      modelType = ModelTypes.withName(modelType.toUpperCase()),
      options = options)
  }
}

object EventStrategySettingsFactory {

  def create(config: Config): EventStrategySettings = {

    val rules: Seq[EventRule] = config.getObjectList(EVENT_RULES).asScala.map(elem => {

      val cfg = elem.toConfig
      val name = cfg.getString(NAME)
      val streamingSource = cfg.getString(STREAMING_SOURCE)
      val typeExpr = cfg.getString(TYPE_EXPRESSION)
      val severityEpxr = cfg.getString(SEVERITY_EXPRESSION)
      val statement = cfg.getString(STATEMENT)
      val sourceIdExpr = cfg.getString(SOURCE_ID_EXPRESSION)

      EventRule(
        eventRuleName = name,
        streamingSource = streamingSource,
        ruleStatement = statement,
        ruleTypeExpr = typeExpr,
        ruleSeverityExpr = severityEpxr,
        ruleSourceIdExpr = sourceIdExpr)
    })

    EventStrategySettings(rules = rules)
  }

}

object MailingStrategySettingsFactory {
  def create(config: Config): MailingStrategySettings = {

    val enableAggregation = if(config.hasPath(ENABLE_AGGREGATION)){
       config.getBoolean(ENABLE_AGGREGATION)
    } else false

    val rules: Seq[MailingRule] = config.getObjectList(MAIL_RULES).asScala.map(elem => {
      val cfg = elem.toConfig

      val name = cfg.getString(NAME)

      val statement = cfg.getString(STATEMENT)
      val subjectExpr = cfg.getString(SUBJECT_EXPR)
      val templatePath = cfg.getString(TEMPLATE_PATH)

      val recipientCfg = cfg.getConfig(RECIPIENT)
      val mailTo = recipientCfg.getString(MAIL_TO)
      val mailCc: Option[String] = recipientCfg.hasPath(MAIL_CC) match {
        case false => None
        case true => Some(recipientCfg.getString(MAIL_CC))
      }
      val mailBcc: Option[String] = recipientCfg.hasPath(MAIL_BCC) match {
        case false => None
        case true => Some(recipientCfg.getString(MAIL_BCC))
      }

      MailingRule(
        mailingRuleName = name,
        ruleStatement = statement,
        subjectStatement = subjectExpr,
        templatePath = templatePath,
        mailTo = mailTo,
        mailCc = mailCc,
        mailBcc = mailBcc)
    })

    MailingStrategySettings(
      rules = rules,
      enableMailAggregation = enableAggregation)
  }
}
