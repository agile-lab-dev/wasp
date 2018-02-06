package it.agilelab.bigdata.wasp.core.messages

import it.agilelab.bigdata.wasp.core.WaspMessage

case class StopBatchJobsMessage() extends WaspMessage

case class CheckJobsBucketMessage() extends WaspMessage
case class BatchJobProcessedMessage(id: String, jobState: String) extends WaspMessage
case class StartBatchJobMessage(id: String) extends WaspMessage
case class BatchJobResult(id:String, result: Boolean) extends WaspMessage
case class StartSchedulersMessage() extends WaspMessage