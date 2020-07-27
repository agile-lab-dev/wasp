package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.collaborator

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master.Protocol.WorkAvailable

class CollaboratorActor(masterGuardian: ActorRef, childFactory: (ActorRef, String, ActorRefFactory) => ActorRef) extends Actor {
  override def receive: Receive = {
    case msg @ WorkAvailable(name) =>
      childFactory(masterGuardian, name, context).forward(msg)
  }
}

object CollaboratorActor {
  def props(masterGuardian: ActorRef, childFactory: (ActorRef, String, ActorRefFactory) => ActorRef) : Props = Props(new CollaboratorActor(masterGuardian, childFactory))
}