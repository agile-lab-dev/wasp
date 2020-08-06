package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import akka.actor.Address
import akka.cluster.UniqueAddress
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Data.Collaborator
import it.agilelab.bigdata.wasp.models.PipegraphModel
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, EitherValues, Matchers, WordSpecLike}

class FifoSchedulingStrategySpec
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
    legacyStreamingComponents = List.empty,
    structuredStreamingComponents = List.empty,
    rtComponents = List.empty
  )

  "Fifo scheduling strategy" must {

    "Notice new nodes added" in {

      val initialSchedulingStrategy = new FifoSchedulingStrategyFactory().create


      val collaborators: Set[Collaborator] = Seq.range(0L, 3L).map { i =>
        Collaborator(UniqueAddress(Address("tcp", "wasp"), i), null, Set("consumer"))
      }.toSet


      Seq.range(0L, 6L).map(_ % collaborators.size).foldLeft(initialSchedulingStrategy) {
        case (currentSchedulingStrategy, currentCollaboratorIndex) =>
          val (chosenCollaborator, updatedSchedulingStrategy) = currentSchedulingStrategy.choose(collaborators, pipegraph).right.value
          chosenCollaborator.address.longUid should be(currentCollaboratorIndex)
          updatedSchedulingStrategy
      }
    }

    "Notice nodes removed" in {

      val initialSchedulingStrategy = new FifoSchedulingStrategyFactory().create


      val collaborators: Set[Collaborator] = Seq.range(0L, 3L).map { i =>
        Collaborator(UniqueAddress(Address("tcp", "wasp"), i), null, Set("consumer"))
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

