package it.agilelab.bigdata.wasp.consumers.spark.strategies

import com.typesafe.config.ConfigFactory
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.MlModelsBroadcastDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

trait Strategy extends Serializable {

  val ALTER_DATE_DAY = "alter_date_day"

  // TODO restore configuration
  var configuration = ConfigFactory.empty()

  var sparkContext: Option[SparkContext] = None

  var mlModelsBroadcast: MlModelsBroadcastDB = MlModelsBroadcastDB.empty

  override def toString: String = this.getClass.getCanonicalName + "{ configuration: " + configuration + " }"


  /**
   *
   * @param dataFrames
   * @return
   */
  def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame
}