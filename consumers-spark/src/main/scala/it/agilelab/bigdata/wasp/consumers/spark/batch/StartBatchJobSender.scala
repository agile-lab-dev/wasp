package it.agilelab.bigdata.wasp.consumers.spark.batch

import it.agilelab.bigdata.wasp.core.WaspSystem
import it.agilelab.bigdata.wasp.core.messages.StartBatchJobMessage
import org.quartz.{Job, JobExecutionContext, JobExecutionException}

import scala.beans.BeanProperty

/**
	* A simple Quartz 2 job that sends a StartBatchJobMessage to the configured SparkConsumersBatchMasterGuardian with the configured
	* job id.
	*
	* @author Nicolò Bidotti
	*/
class StartBatchJobSender() extends Job {
	@BeanProperty
	var jobId: String = _ // automatically populated by quartz with the corresponding JobDataMap key-value
	@BeanProperty
	var sparkConsumersBatchMasterGuardianActorPath: String = _ // automatically populated by quartz with the corresponding JobDataMap key-value
	
	@throws[JobExecutionException]
	def execute(context: JobExecutionContext): Unit = {
		val batchJobActorRef = WaspSystem.actorSystem.actorSelection(getSparkConsumersBatchMasterGuardianActorPath)
		batchJobActorRef ! StartBatchJobMessage(getJobId)
	}
}
