package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.pipegraph

sealed trait State

object State {

  case object WaitingForWork extends State
  case object RequestingWork extends State
  case object Activating extends State
  case object Activated extends State
  case object Materializing extends State
  case object Materialized extends State
  case object Monitoring extends State
  case object Monitored extends State
  case object Stopping extends State
  case object Stopped extends State

}
