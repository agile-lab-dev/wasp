package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.{ ObjectInputStream, ObjectOutputStream }

import org.apache.hadoop.conf.Configuration

/**
  * [[Serializable]] wrapper for a Hadoop [[Configuration]]
  */
class HadoopConfiguration(@transient var value: Configuration) extends Serializable {
  private def writeObject(out: ObjectOutputStream): Unit =
    value.write(out)

  private def readObject(in: ObjectInputStream): Unit = {
    value = new Configuration(false)
    value.readFields(in)
  }
}
