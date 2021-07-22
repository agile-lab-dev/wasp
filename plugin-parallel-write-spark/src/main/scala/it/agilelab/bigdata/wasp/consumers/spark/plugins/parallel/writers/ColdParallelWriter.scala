package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.writers
import it.agilelab.bigdata.microservicecatalog.entity.WriteExecutionPlanResponseBody
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.{ParallelWrite, WriterDetails}
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.utils.{HadoopS3aUtil, HadoopS3Utils}
import org.apache.spark.sql.DataFrame

case class ColdParallelWriter(format: String, parallelWriteDetails: ParallelWrite) extends ParallelWriter {
  override def write(writeExecutionPlan: WriteExecutionPlanResponseBody, df: DataFrame): Unit = {
    val s3path: String = HadoopS3Utils.useS3aScheme(writeExecutionPlan.writeUri).toString()
    if (new HadoopS3aUtil(df.sparkSession.sparkContext.hadoopConfiguration, writeExecutionPlan.temporaryCredentials.w).performBulkHadoopCfgSetup.isFailure)
      throw new Exception("Failed Hadoop settings configuration")

    write(df, s3path)
  }

  private def write(df: DataFrame, s3path: String): Unit = {
    df.write
      .mode(parallelWriteDetails.saveMode)
      .format(format)
      .partitionBy(parallelWriteDetails.partitionBy.getOrElse(Nil):_*)
      .save(s3path)
  }
}
