package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import com.typesafe.config.Config
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Data.Collaborator
import it.agilelab.bigdata.wasp.models.PipegraphModel

case class NodeLabelsSchedulingStrategy(data: Map[Set[String], SchedulingStrategy], tieBreakerFactory: SchedulingStrategyFactory) extends SchedulingStrategy {
  override def choose(members: Set[Collaborator], pipegraph: PipegraphModel): SchedulingStrategy.SchedulingStrategyOutcome = {

    val availableCollaborators: Set[Collaborator] = members.filter { collaborator =>
      pipegraph.labels.forall(label => collaborator.roles.contains(label))
    }

    availableCollaborators match {
      case collaborators if collaborators.isEmpty =>
        noNodeAvailableOutcome(pipegraph)
      case collaborators =>
        val tieBreaker = data.getOrElse(pipegraph.labels, tieBreakerFactory.create)
        tieBreaker.choose(collaborators, pipegraph) match {
          case Left((error, newTieBreaker)) =>
            val updated = data.updated(pipegraph.labels, newTieBreaker)
            Left((error, this.copy(data = updated)))
          case Right((chosen, newTieBreaker)) =>
            val updated = data.updated(pipegraph.labels, newTieBreaker)
            Right((chosen, this.copy(data = updated)))
        }

    }


  }


  private def noNodeAvailableOutcome(pipegraph: PipegraphModel): SchedulingStrategy.SchedulingStrategyOutcome =
    Left((s"No node is able to schedule ${
      pipegraph.name
    } with labels ${
      pipegraph.labels.mkString(",")
    }", this))


}

class NodeLabelsSchedulingStrategyFactory extends SchedulingStrategyFactory {

  var tieBreakerFactory: SchedulingStrategyFactory = _

  override def inform(factoryParams: Config): SchedulingStrategyFactory = {
    tieBreakerFactory = tieBreakerFactoryFromConfig(factoryParams)
    this
  }

  def tieBreakerFactoryFromConfig(config: Config): SchedulingStrategyFactory = {
    if (config.hasPath("tie-breaker")) {
      val innerConfig = config.getConfig("tie-breaker")

      if (innerConfig.hasPath("class-name")) {
        Class.forName(innerConfig.getString("class-name")).getDeclaredConstructor().newInstance().asInstanceOf[SchedulingStrategyFactory].inform(innerConfig)
      } else {
        throw new Exception("Expected a [scheduling-strategy.class-name] config key")
      }

    } else {
      new FifoSchedulingStrategyFactory()
    }
  }

  override def create: SchedulingStrategy = NodeLabelsSchedulingStrategy(Map.empty, tieBreakerFactory)
}
