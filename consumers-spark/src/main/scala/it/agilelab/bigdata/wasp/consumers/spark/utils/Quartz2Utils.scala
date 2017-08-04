package it.agilelab.bigdata.wasp.consumers.spark.utils

import it.agilelab.bigdata.wasp.consumers.spark.batch.StartBatchJobSender
import it.agilelab.bigdata.wasp.core.bl.ConfigBL
import it.agilelab.bigdata.wasp.core.models.BatchSchedulerModel
import org.quartz.{JobDetail, Scheduler, Trigger}
import org.quartz.JobBuilder._
import org.quartz.TriggerBuilder._
import org.quartz.CronScheduleBuilder._
import org.quartz.impl.StdSchedulerFactory


/**
	* Utilities for Quartz 2 scheduler.
	*
	* @author Nicolò Bidotti
	*/
object Quartz2Utils {
	def buildScheduler(): Scheduler = {
		// create standard scheduler
		val sf = new StdSchedulerFactory
		val scheduler = sf.getScheduler
		
		// scheduler will not execute jobs until it has been started
		scheduler.start()
		
		scheduler
	}
	
	implicit class BatchSchedulerModelQuartz2Support(schedulerModel: BatchSchedulerModel) {
		private val batchJobModel = ConfigBL.batchJobBL.getById(schedulerModel.batchJob.get.getValue.toHexString).get
		
		def getQuartzJob(batchMasterGuardianActorPath: String): JobDetail = {
			val job = newJob(classOf[StartBatchJobSender])
				.withIdentity(batchJobModel.name, batchJobModel.owner)
				.usingJobData("jobId", batchJobModel._id.get.getValue.toHexString)
				.usingJobData("batchMasterGuardianActorPath", batchMasterGuardianActorPath)
				.build()
			
			job
		}
		
		def getQuartzTrigger: Trigger = {
			
			val trigger = newTrigger()
				.withIdentity(schedulerModel.name, batchJobModel.owner)
				.withSchedule(cronSchedule(schedulerModel.cronExpression))
				.startNow()
				.build()
			
			trigger
		}
	}
}
