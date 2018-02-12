package it.agilelab.bigdata.wasp.core.messages

import akka.actor.ActorRef
import akka.http.scaladsl.model.HttpMethod
import it.agilelab.bigdata.wasp.core.WaspMessage
import spray.json.JsValue

trait MasterGuardianMessage extends WaspMessage { val name: String }

trait PipegraphMessage extends MasterGuardianMessage
trait ProducerMessage extends MasterGuardianMessage
trait ETLMessage extends MasterGuardianMessage { val etlName: String }
trait BatchJobMessage extends MasterGuardianMessage

case class RemovePipegraph(override val name: String) extends PipegraphMessage
case class StartPipegraph(override val name: String) extends PipegraphMessage
case class StopPipegraph(override val name: String) extends PipegraphMessage
case object RestartPipegraphs extends MasterGuardianMessage { val name = null }
case class AddRemoteProducer(override val name: String, remoteProducer: ActorRef) extends ProducerMessage
case class RemoveRemoteProducer(override val name: String, remoteProducer: ActorRef) extends ProducerMessage
case class StartProducer(override val name: String) extends ProducerMessage
case class StopProducer(override val name: String) extends ProducerMessage
case class RestProducerRequest
  (override val name: String, httpMethod: HttpMethod, data: JsValue, modelKey: ModelKey) extends ProducerMessage

case class StartETL(override val name: String, override val etlName: String) extends ETLMessage
case class StopETL(override val name: String, override val etlName: String) extends ETLMessage

case class StartBatchJob(override val name: String) extends BatchJobMessage
case class StartPendingBatchJobs(override val name: String) extends MasterGuardianMessage