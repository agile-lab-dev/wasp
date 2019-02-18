package it.agilelab.bigdata.wasp.core.eventengine.eventproducers


/**
  * Event rules store a group of SQL statements which indicate whether an Event occurred or not, as well as how to
  * enrich it with context information
  * @param eventRuleName is the name of the event rule
  * @param ruleStreamingSource is the name of the input data which are being analyzed
  * @param ruleStatement defines whether an Event occurred or not
  * @param ruleTypeExpr enrich the Event object with the type field
  * @param ruleSeverityExpr enrich the Event object with the severity field
  * @param ruleSourceIdExpr enrich the Event object with the sourceId field
  */
case class EventRule(
                      eventRuleName: String,
                      streamingSource: String,
                      ruleStatement: String, //sql expression
                      ruleTypeExpr: String, //sql expression
                      ruleSeverityExpr: String, //sql expression
                      ruleSourceIdExpr: String)
