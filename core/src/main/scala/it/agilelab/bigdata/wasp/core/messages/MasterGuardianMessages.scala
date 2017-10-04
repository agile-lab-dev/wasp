package it.agilelab.bigdata.wasp.core.messages

import akka.actor.ActorRef
import akka.http.scaladsl.model.HttpMethod
import it.agilelab.bigdata.wasp.core.WaspMessage
import spray.json.JsValue

trait MasterGuardianMessage extends WaspMessage { val id: String }

trait PipegraphMessage extends MasterGuardianMessage
trait ProducerMessage extends MasterGuardianMessage
trait ETLMessage extends MasterGuardianMessage { val etlName: String }
trait BatchJobMessage extends MasterGuardianMessage

case class RemovePipegraph(override val id: String) extends PipegraphMessage
case class StartPipegraph(override val id: String) extends PipegraphMessage
case class StopPipegraph(override val id: String) extends PipegraphMessage
case object RestartPipegraphs extends MasterGuardianMessage { val id = null }
case class AddRemoteProducer(override val id: String, remoteProducer: ActorRef) extends ProducerMessage
case class RemoveRemoteProducer(override val id: String, remoteProducer: ActorRef) extends ProducerMessage
case class StartProducer(override val id: String) extends ProducerMessage
case class StopProducer(override val id: String) extends ProducerMessage
case class RestProducerRequest
  (override val id: String, httpMethod: HttpMethod, data: JsValue, mlModelId: String) extends ProducerMessage

case class StartETL(override val id: String, override val etlName: String) extends ETLMessage
case class StopETL(override val id: String, override val etlName: String) extends ETLMessage

case class StartBatchJob(override val id: String) extends BatchJobMessage
case class StartPendingBatchJobs(override val id: String) extends MasterGuardianMessage