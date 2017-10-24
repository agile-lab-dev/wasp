package it.agilelab.bigdata.wasp.core.messages

import it.agilelab.bigdata.wasp.core.WaspMessage


case class WaspMessageEnvelope[K, V](topic: String, key: K, messages: V*) extends WaspMessage