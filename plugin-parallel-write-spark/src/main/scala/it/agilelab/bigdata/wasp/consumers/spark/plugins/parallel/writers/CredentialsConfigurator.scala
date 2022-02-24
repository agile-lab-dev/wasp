package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.entity.WriteExecutionPlanResponseBody
import org.apache.hadoop.conf.Configuration

trait CredentialsConfigurator {
  def configureCredentials(writeExecutionPlanResponseBody: WriteExecutionPlanResponseBody, configuration: Configuration)
}

object CredentialsConfigurator {
  def coldAreaCredentialsPersisterConfigurator: CredentialsConfigurator = ColdAreaCredentialsPersister
  def hadoopConfigConfigurator: CredentialsConfigurator = ???
}
