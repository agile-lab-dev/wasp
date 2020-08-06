package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Data.Collaborator
import it.agilelab.bigdata.wasp.models.PipegraphModel

import scala.collection.immutable.Queue

case class FifoSchedulingStrategy(queue: Queue[Collaborator]) extends SchedulingStrategy {

  override def choose(members: Set[Collaborator], pipegraph: PipegraphModel): SchedulingStrategy.SchedulingStrategyOutcome = {

    val newMembers = members.filterNot(queue.contains(_))
    val onlyExistingNodes: Queue[Collaborator] = queue.filter(c => members.contains((c)))

    val all = onlyExistingNodes ++ newMembers

    val (chosen, remaining) = all.dequeue

    Right((chosen, this.copy(queue = remaining.enqueue(chosen))))

  }
}


