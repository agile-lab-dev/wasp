package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Data.Collaborator
import it.agilelab.bigdata.wasp.models.PipegraphModel

class LowestUUIDNodeSchedulingStrategy extends SchedulingStrategy {

  override def choose(members: Set[Collaborator], pipegraph: PipegraphModel): SchedulingStrategy.SchedulingStrategyOutcome = {
    Right((members.toList.minBy(_.address.longUid), this))
  }

}

class LowestUUIDNodeSchedulingStrategyFactory extends SchedulingStrategyFactory {
  override def create: SchedulingStrategy = new LowestUUIDNodeSchedulingStrategy
}
