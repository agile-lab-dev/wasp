package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import scala.collection.immutable.Queue

class FifoSchedulingStrategyFactory extends SchedulingStrategyFactory {
  override def create: SchedulingStrategy = FifoSchedulingStrategy(Queue.empty)
}
