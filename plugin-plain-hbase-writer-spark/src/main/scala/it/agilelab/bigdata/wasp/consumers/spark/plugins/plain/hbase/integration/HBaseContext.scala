package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.{SerializableWritable, SparkContext}

//scalastyle:off
class HBaseContext(@transient val sc: SparkContext,
  @transient val config: Configuration,
  val tmpHdfsConfgFile: String = null) extends Serializable with Logging {

  @transient var tmpHdfsConfiguration: Configuration = config
  @transient var appliedCredentials = false
  @transient val job = Job.getInstance(config)

  val broadcastedConf: Broadcast[SerializableWritable[Configuration]] = sc.broadcast(new SerializableWritable(config))

  LatestHBaseContextCache.latest = this

  if (tmpHdfsConfgFile != null && config != null) {
    val fs = FileSystem.newInstance(config)
    val tmpPath = new Path(tmpHdfsConfgFile)
    if (!fs.exists(tmpPath)) {
      val outputStream = fs.create(tmpPath)
      config.write(outputStream)
      outputStream.close()
    } else {
      logWarning("tmpHdfsConfigDir " + tmpHdfsConfgFile + " exist!!")
    }
  }

  def getConf(configBroadcast: Broadcast[SerializableWritable[Configuration]] = broadcastedConf): Configuration = {

    if (tmpHdfsConfiguration == null && tmpHdfsConfgFile != null) {
      val fs = FileSystem.newInstance(SparkHadoopUtil.get.conf)
      val inputStream = fs.open(new Path(tmpHdfsConfgFile))
      tmpHdfsConfiguration = new Configuration(false)
      tmpHdfsConfiguration.readFields(inputStream)
      inputStream.close()
    }

    if (tmpHdfsConfiguration == null) {
      try {
        tmpHdfsConfiguration = configBroadcast.value.value
      } catch {
        case ex: Exception => logError("Unable to getConfig from broadcast", ex)
      }
    }
    tmpHdfsConfiguration
  }


}

object LatestHBaseContextCache {
  var latest: HBaseContext = null
}
