package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.master

/**
  * Trait marking objects representing [[SparkConsumersStreamingMasterGuardian]] State
  */
sealed trait State

object State {

  /**
    * Idle starting state
    */
  case object Idle extends State

  /**
    * Recovering state from database
    */
  case object Initializing extends State

  /**
    * Ready to serve requests
    */
  case object Initialized extends State

}