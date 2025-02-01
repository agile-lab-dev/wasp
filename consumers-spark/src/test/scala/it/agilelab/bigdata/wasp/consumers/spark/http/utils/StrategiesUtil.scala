package it.agilelab.bigdata.wasp.consumers.spark.http.utils

import it.agilelab.bigdata.wasp.consumers.spark.http.data.FromKafka
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.utils.SparkSuite
import org.apache.spark.sql.DataFrame

trait StrategiesUtil { _: SparkSuite =>

  protected def runStrategy(sut: Strategy, readerKey: ReaderKey, data: FromKafka): DataFrame    =
    sut.transform(Map(readerKey -> spark.createDataFrame(spark.sparkContext.parallelize(Seq(data)))))
}
