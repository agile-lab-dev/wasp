package it.agilelab.bigdata.wasp.core.messages

import akka.actor.ActorRef

case class TelemetryMessageSource(
                                   messageId: String,
                                   sourceId: String,
                                   timestamp: String,
                                   description: String,
                                   startOffset: Map[String, Map[String, Long]], //Hack to parse the json and store the topic name
                                   endOffset: Map[String, Map[String, Long]]
                                 )
case class TelemetryMessageSourcesSummary(streamingQueriesProgress: Seq[TelemetryMessageSource])

case class TelemetryActorRedirection(actorRef: ActorRef)
case object MessageTimeout



