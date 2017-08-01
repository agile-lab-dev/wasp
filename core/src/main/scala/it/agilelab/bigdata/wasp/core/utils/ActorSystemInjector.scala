package it.agilelab.bigdata.wasp.core.utils

import it.agilelab.bigdata.wasp.core.WaspSystem


trait ActorSystemInjector {

  def actorSystemName = "WASP"

  WaspSystem.initializeActorSystem(actorSystemName)
  
}