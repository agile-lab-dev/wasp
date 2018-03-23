package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

sealed trait State

object State {

  case object WaitingToBeActivated extends State

  case object WaitingToBeMaterialized extends State

  case object WaitingToBeMonitored extends State

}