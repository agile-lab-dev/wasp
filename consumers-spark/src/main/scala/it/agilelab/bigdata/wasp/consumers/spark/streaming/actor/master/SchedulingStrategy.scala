package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Data.Collaborator
import it.agilelab.bigdata.wasp.models.PipegraphModel

object SchedulingStrategy {

  type SchedulingStrategyOutcome = Either[(String, SchedulingStrategy), (Collaborator, SchedulingStrategy)]

}

trait SchedulingStrategy {

  def choose(members: Set[Collaborator], pipegraph: PipegraphModel): SchedulingStrategy.SchedulingStrategyOutcome

}
