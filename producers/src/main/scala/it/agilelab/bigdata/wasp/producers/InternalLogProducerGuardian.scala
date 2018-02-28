/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package it.agilelab.bigdata.wasp.producers

import java.time.format.DateTimeFormatter

import akka.actor._
import akka.routing.BalancingPool
import it.agilelab.bigdata.wasp.core.{SystemPipegraphs, WaspSystem}
import it.agilelab.bigdata.wasp.core.WaspSystem._
import it.agilelab.bigdata.wasp.core.bl.{ProducerBL, TopicBL}
import it.agilelab.bigdata.wasp.core.kafka.CheckOrCreateTopic
import it.agilelab.bigdata.wasp.core.logging.Logging.LogEvent
import it.agilelab.bigdata.wasp.core.models.TopicModel
import it.agilelab.bigdata.wasp.core.utils.ConfigManager

import scala.util.parsing.json.{JSONFormat, JSONObject}

// producerName is an empty string because we override initialize
final class InternalLogProducerGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL})
  extends ProducerGuardian(env, "") {

  val name = InternalLogProducerGuardian.name

  var producerActor: Option[ActorRef] = None

  def startChildActors() {
    logger.info("Executing startChildActor method")
    producerActor = Some(context.actorOf(Props(new InternalLogProducerActor(kafka_router, associatedTopic))))
  }

  override def initialized: Actor.Receive = super.initialized orElse loggerInitialized

  def loggerInitialized: Actor.Receive = {
    case e: LogEvent =>
      if (producerActor.isDefined)
        producerActor.get forward e
  }

  override def initialize(): Either[String, Unit] = {

    val producerOption = env.producerBL.getByName(name)

    if (producerOption.isDefined) {
        producer = producerOption.get
        if (producer.hasOutput) {
          val topicOption = env.producerBL.getTopic(topicBL = env.topicBL, producer)

          associatedTopic = topicOption
          logger.info(s"Producer '$name': topic found: $associatedTopic")
          val result = ??[Boolean](WaspSystem.kafkaAdminActor, CheckOrCreateTopic(topicOption.get.name, topicOption.get.partitions, topicOption.get.replicas))
          if (result) {
            router_name = s"kafka-ingestion-router-$name-${producer.name}-${System.currentTimeMillis()}"
            kafka_router = actorSystem.actorOf(BalancingPool(5).props(Props(new KafkaPublisherActor(ConfigManager.getKafkaConfig))), router_name)
            context become initialized
            startChildActors()

            env.producerBL.setIsActive(producer, isActive = true)

            Right()
          } else {
            val msg = s"Producer '$name': error creating topic " + topicOption.get.name
            logger.error(msg)
            Left(msg)
          }
        } else {
          val msg = s"Producer '$name': error undefined topic"
          logger.error(msg)
          Left(msg)
        }
      } else {
        val msg = s"Producer '$name': error not defined"
        logger.error(msg)
        Left(msg)
      }
  }
}

object InternalLogProducerGuardian {
  val name = SystemPipegraphs.loggerProducer.name
}

private class InternalLogProducerActor(kafka_router: ActorRef, topic: Option[TopicModel]) extends ProducerActor[LogEvent](kafka_router, topic) {

  override def receive: Actor.Receive = super.receive orElse loggerReceive

  def loggerReceive(): Actor.Receive = {
    case event : LogEvent => sendMessage(event)
  }

  override def mainTask() = {
    /* We don't have a task here because it's a system pipeline */
  }

  override def generateOutputJsonMessage(event: LogEvent) =
    JSONObject(Map( "log_source" -> event.loggerName,
                    "log_level" -> event.level.toString,
                    "message" -> event.message,
                    "timestamp" -> DateTimeFormatter.ISO_INSTANT.format(event.timestamp),
                    "thread" -> event.thread,
                    "cause" -> event.maybeCause.getOrElse(""),
                    "stack_trace" ->  event.maybeStackTrace.getOrElse(""))).toString(JSONFormat.defaultFormatter)
}