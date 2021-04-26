package it.agilelab.bigdata.wasp.core

trait AroundLaunch {
  def beforeLaunch(): Unit

  def afterLaunch(): Unit
}
