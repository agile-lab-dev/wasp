package it.agilelab.bigdata.wasp.consumers.spark

import akka.actor.ActorRef
import akka.camel.{CamelMessage, Consumer}
import it.agilelab.bigdata.wasp.consumers.spark.batch.StartBatchJobMessage
import it.agilelab.bigdata.wasp.core.logging.WaspLogger
import it.agilelab.bigdata.wasp.core.models.BatchSchedulerModel

object CamelQuartz2Scheduler {
  val name = "CamelQuartz2Scheduler"
}

class CamelQuartz2Scheduler(batchMasterGuardian: ActorRef, schedulerModel: BatchSchedulerModel) extends Consumer {
  val logger = WaspLogger(this.getClass.getName)
    val batchJobId = schedulerModel.batchJob
    //TODO implementare integrazione options (eventualmente cambiare struttura model)
    def endpointUri = s"quartz2://${schedulerModel.quartzUri}"

    def receive = {

      case msg: CamelMessage =>
        if(batchJobId.isDefined) {
          batchMasterGuardian ! StartBatchJobMessage(batchJobId.get.stringify)
        } else {
          logger.warn("Batch scheduler has no batch job id")
        }

    }
}
