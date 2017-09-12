package it.agilelab.bigdata.wasp.consumers.spark.readers

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame


/**
  * Created by Mattia Bertorello on 10/09/15.
  */
trait StaticReader {
  val name: String
  val readerType: String

  def read(sc: SparkContext): DataFrame
}