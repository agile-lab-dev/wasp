package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import akka.actor.Address
import akka.cluster.UniqueAddress
import com.typesafe.config.{Config, ConfigFactory}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Data.Collaborator
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.SchedulingStrategy.SchedulingStrategyOutcome
import it.agilelab.bigdata.wasp.models.PipegraphModel
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, EitherValues, Matchers, WordSpecLike}


class MockedFactory extends SchedulingStrategyFactory {


  override def inform(factoryParams: Config): SchedulingStrategyFactory = {
    MockedFactoryHolder.informed = factoryParams
    this
  }

  override def create: SchedulingStrategy = new SchedulingStrategy {
    override def choose(members: Set[Collaborator], pipegraph: PipegraphModel): SchedulingStrategyOutcome = ???
  }
}

object MockedFactoryHolder {
  var informed: Config = _
}

class NodeLabelsSchedulingStrategySpec
  extends WordSpecLike
    with BeforeAndAfterAll
    with Matchers
    with Eventually
    with EitherValues {

  val pipegraph: PipegraphModel = PipegraphModel(
    name = "Name",
    description = "description",
    owner = "owner",
    isSystem = false,
    creationTime = 0L,
    structuredStreamingComponents = List.empty,
    labels = Set("label-1", "label-2")
  )

  "Factory should be able to create instances correctly" in {


    // should at least not throw
    val informed = new NodeLabelsSchedulingStrategyFactory().inform(ConfigFactory.empty())
    informed.create
    // did not throw, everything ok


    val result = new NodeLabelsSchedulingStrategyFactory().inform(ConfigFactory.parseString(
      """
        |tie-breaker{
        |  class-name = "it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.MockedFactory"
        |  other-param = "ciccio"
        |}
        |""".stripMargin))

    result.create

    MockedFactoryHolder.informed.getString("other-param") should be("ciccio")

  }

  "Node label scheduling strategy" must {

    "Correctly maintain downstream tie breaker strategies " in {
      val initialSchedulingStrategy: SchedulingStrategy = NodeLabelsSchedulingStrategy(Map.empty, new FifoSchedulingStrategyFactory)


      val unschedulableCollaborators: Set[Collaborator] = Seq.range(0L, 3L).map { i =>
        Collaborator(UniqueAddress(Address("tcp", "wasp"), i), null, Set("consumer", "label-1"))
      }.toSet

      val schedulableCollaborators = Seq.range(0L, 3L).map { i =>
        Collaborator(UniqueAddress(Address("tcp", "wasp"), i), null, Set("consumer", "label-1", "label-2"))
      }.toSet

      val allCollaborators = schedulableCollaborators ++ unschedulableCollaborators


      val beforeLosingTwoOfTheThreeSchedulableNodesSchedulingStrategy = Seq.range(0L, 100L).map(_ % schedulableCollaborators.size).foldLeft(initialSchedulingStrategy) {
        case (currentSchedulingStrategy, currentCollaboratorIndex) =>
          val (chosenCollaborator, updatedSchedulingStrategy) = currentSchedulingStrategy.choose(allCollaborators, pipegraph).right.value
          chosenCollaborator.address.longUid should be(allCollaborators.toSeq.apply(currentCollaboratorIndex.toInt).address.longUid)
          updatedSchedulingStrategy
      }

      val theOnlyStillSchedulableCollaborator = Collaborator(UniqueAddress(Address("tcp", "wasp"), 199L), null, Set("consumer", "label-1", "label-2"))
      val withOnlyOneStillSchedulable: Set[Collaborator] = unschedulableCollaborators + theOnlyStillSchedulableCollaborator


      val beforeLosingTheLastSchedulableNode = Seq.range(0L, 100L).map(_ => 0).foldLeft(beforeLosingTwoOfTheThreeSchedulableNodesSchedulingStrategy) {
        case (currentSchedulingStrategy, currentCollaboratorIndex) =>
          val (chosenCollaborator, updatedSchedulingStrategy) = currentSchedulingStrategy.choose(withOnlyOneStillSchedulable, pipegraph).right.value
          chosenCollaborator.address.longUid should be(theOnlyStillSchedulableCollaborator.address.longUid)
          updatedSchedulingStrategy
      }

      val noSchedulableNodes = unschedulableCollaborators


      Seq.range(0L, 100L).map(_ => 0).foldLeft(beforeLosingTheLastSchedulableNode) {
        case (currentSchedulingStrategy, currentCollaboratorIndex) =>
          val (chosenCollaborator, updatedSchedulingStrategy) = currentSchedulingStrategy.choose(noSchedulableNodes, pipegraph).left.value
          chosenCollaborator should be("No node is able to schedule Name with labels label-1,label-2")
          updatedSchedulingStrategy
      }

    }

    "Report failure if no nodes are able to schedule the pipegraph" in {
      val initialSchedulingStrategy: SchedulingStrategy = NodeLabelsSchedulingStrategy(Map.empty, new FifoSchedulingStrategyFactory)


      val collaborators: Set[Collaborator] = Seq.range(0L, 3L).map { i =>
        Collaborator(UniqueAddress(Address("tcp", "wasp"), i), null, Set("consumer", "label-1"))
      }.toSet


      Seq.range(0L, 6L).map(_ % collaborators.size).foldLeft(initialSchedulingStrategy) {
        case (currentSchedulingStrategy, currentCollaboratorIndex) =>
          val (error, updatedSchedulingStrategy) = currentSchedulingStrategy.choose(collaborators, pipegraph).left.value
          error should be("No node is able to schedule Name with labels label-1,label-2")
          updatedSchedulingStrategy
      }
    }

    "break ties using Lowest Node UUID strategy" must {

      "choose always the node with the lowest UUID" in {

        val initialSchedulingStrategy: SchedulingStrategy = NodeLabelsSchedulingStrategy(Map.empty, new LowestUUIDNodeSchedulingStrategyFactory)


        val collaborators: Set[Collaborator] = Seq.range(0L, 3L).map { i =>
          Collaborator(UniqueAddress(Address("tcp", "wasp"), i), null, Set("consumer", "label-1", "label-2"))
        }.toSet

        Seq.range(0L, 6L).foldLeft(initialSchedulingStrategy) {
          case (currentSchedulingStrategy, _) =>
            val (chosenCollaborator, updatedSchedulingStrategy) = currentSchedulingStrategy.choose(collaborators, pipegraph).right.value
            chosenCollaborator.address.longUid should be(0)
            updatedSchedulingStrategy
        }


        val collaboratorsWithoutTheFirst = collaborators.filterNot(_.address.longUid == 0)

        Seq.range(0L, 6L).foldLeft(initialSchedulingStrategy) {
          case (currentSchedulingStrategy, _) =>
            val (chosenCollaborator, updatedSchedulingStrategy) = currentSchedulingStrategy.choose(collaboratorsWithoutTheFirst, pipegraph).right.value
            chosenCollaborator.address.longUid should be(1)
            updatedSchedulingStrategy
        }
      }
    }
    "break ties using Fifo Strategy" must {

      "Notice new nodes added" in {

        val initialSchedulingStrategy: SchedulingStrategy = NodeLabelsSchedulingStrategy(Map.empty, new FifoSchedulingStrategyFactory)


        val collaborators: Set[Collaborator] = Seq.range(0L, 3L).map { i =>
          Collaborator(UniqueAddress(Address("tcp", "wasp"), i), null, Set("consumer", "label-1", "label-2"))
        }.toSet


        Seq.range(0L, 6L).map(_ % collaborators.size).foldLeft(initialSchedulingStrategy) {
          case (currentSchedulingStrategy, currentCollaboratorIndex) =>
            val (chosenCollaborator, updatedSchedulingStrategy) = currentSchedulingStrategy.choose(collaborators, pipegraph).right.value
            chosenCollaborator.address.longUid should be(currentCollaboratorIndex)
            updatedSchedulingStrategy
        }
      }

      "Notice nodes removed" in {

        val initialSchedulingStrategy: SchedulingStrategy = NodeLabelsSchedulingStrategy(Map.empty, new FifoSchedulingStrategyFactory)


        val collaborators: Set[Collaborator] = Seq.range(0L, 3L).map { i =>
          Collaborator(UniqueAddress(Address("tcp", "wasp"), i), null, Set("consumer", "label-1", "label-2"))
        }.toSet


        val primedStrategy = Seq.range(0L, 6L).map(_ % collaborators.size).foldLeft(initialSchedulingStrategy) {
          case (currentSchedulingStrategy, currentCollaboratorIndex) =>
            val (chosenCollaborator, updatedSchedulingStrategy) = currentSchedulingStrategy.choose(collaborators, pipegraph).right.value
            chosenCollaborator.address.longUid should be(currentCollaboratorIndex)
            updatedSchedulingStrategy
        }

        val withoutCollaborator0 = collaborators.filterNot(_.address.longUid == 0)

        val (shouldBeCollaborator1, strategyThatWillSelectCollaborator2) = primedStrategy.choose(withoutCollaborator0, pipegraph).right.value

        shouldBeCollaborator1.address.longUid should be(1)


        val (shouldBeCollaborator2, strategyThatWillSelectCollaborator1Again) = strategyThatWillSelectCollaborator2.choose(withoutCollaborator0, pipegraph).right.value

        shouldBeCollaborator2.address.longUid should be(2)

        val (shouldBeCollaborator1Again, _) = strategyThatWillSelectCollaborator1Again.choose(withoutCollaborator0, pipegraph).right.value

        shouldBeCollaborator1Again.address.longUid should be(1)

      }
    }
  }


}

