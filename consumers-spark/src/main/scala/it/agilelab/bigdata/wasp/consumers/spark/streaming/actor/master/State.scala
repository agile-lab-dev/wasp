package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

sealed trait State

object State {

  case object Idle extends State

  case object Initializing extends State

  case object Initialized extends State

}