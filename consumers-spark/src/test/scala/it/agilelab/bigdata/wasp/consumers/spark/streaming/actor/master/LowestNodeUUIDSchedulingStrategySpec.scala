package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

import akka.actor.Address
import akka.cluster.UniqueAddress
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Data.Collaborator
import it.agilelab.bigdata.wasp.models.PipegraphModel
import org.scalatest.concurrent.Eventually
import org.scalatest.{BeforeAndAfterAll, EitherValues, Matchers, WordSpecLike}

class LowestNodeUUIDSchedulingStrategySpec
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
    structuredStreamingComponents = List.empty
  )

  "Lowest Node UUID scheduling strategy" must {

    "choose always the node with the lowest UUID" in {

      val initialSchedulingStrategy = new LowestUUIDNodeSchedulingStrategyFactory().create


      val collaborators: Set[Collaborator] = Seq.range(0L, 3L).map { i =>
        Collaborator(UniqueAddress(Address("tcp", "wasp"), i), null, Set("consumer"))
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

}

