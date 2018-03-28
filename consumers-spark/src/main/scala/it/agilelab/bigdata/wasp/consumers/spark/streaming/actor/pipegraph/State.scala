package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph

sealed trait State

object State {

  /**
    * [[PipegraphGuardian]] is waiting for work
    */
  case object WaitingForWork extends State

  /**
    * [[PipegraphGuardian]] requested work and is waiting for a response
    */
  case object RequestingWork extends State

  /**
    * [[PipegraphGuardian]] is activating a pipegraph
    */
  case object Activating extends State

  /**
    * [[PipegraphGuardian]] activated the pipegraph.
    *
    * @note it is a safe state with no in-flight request, so we can compute if we should retry activation or go to
    *       materializing
    */
  case object Activated extends State

  /**
    * [[PipegraphGuardian]] is materializing a pipegraph
    */
  case object Materializing extends State

  /**
    * [[PipegraphGuardian]] materialized a pipegraph
    * @note it is a safe state with no in-flight request, so we can compute if we should retry materialization or go to
    *       Monitoring
    */
  case object Materialized extends State

  /**
    * [[PipegraphGuardian]] is monitoring a pipegraph
    *
    */
  case object Monitoring extends State

  /**
    * [[PipegraphGuardian]] monitored a pipegraph
    * @note it is a safe state with no in-flight request, so we can compute if we should retry materialization or go to
    *       Monitoring or to Activating
    */
  case object Monitored extends State

  /**
    * [[PipegraphGuardian]] is stopping a pipegraph
    */
  case object Stopping extends State

  /**
    * [[PipegraphGuardian]] is stopped a pipegraph
    *
    * @note it is a safe state with no in-flight request, so we can safely shutdown
    */
  case object Stopped extends State

}
