package it.agilelab.bigdata.wasp.core.messages

import it.agilelab.bigdata.wasp.core.WaspMessage


case object RestartConsumers extends WaspMessage

case object OutputStreamInitialized extends WaspMessage

case object StopProcessingComponent extends WaspMessage
