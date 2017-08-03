package it.agilelab.bigdata.wasp.consumers.spark.strategies

import java.util.{Calendar, Date}

import it.agilelab.bigdata.wasp.consumers.spark.MlModels.MlModelsBroadcastDB
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.streaming.dstream.DStream

case class ReaderKey(sourceTypeName: String, name: String)

trait Strategy {

  val ALTER_DATE_DAY = "alter_date_day"

  // TODO restore configuration
  var configuration = Map[String, Any]()

  var sparkContext: Option[SparkContext] = None

  var mlModelsBroadcast: MlModelsBroadcastDB = MlModelsBroadcastDB.empty

  override def toString: String = this.getClass.getCanonicalName + "{ configuration: " + configuration + " }"

  protected def alterDateDay(date: Date): Date =
    configuration.get(ALTER_DATE_DAY).map(ad => {
      val calendar = Calendar.getInstance
      calendar.setTime(date)
      calendar.add(Calendar.DATE, ad.asInstanceOf[Int])
      calendar.getTime
    }).getOrElse(date)

  /**
   *
   * @param dataFrames
   * @return
   */
  def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame

  //TODO Implement join in ConsumenrETLActor
  def join(dsStreams: Map[ReaderKey, DStream[String]]): DStream[String] = dsStreams.head._2


}