package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import com.typesafe.config.Config

trait SchedulingStrategyFactory {

  def inform(factoryParams: Config): SchedulingStrategyFactory = this

  def create: SchedulingStrategy
}
