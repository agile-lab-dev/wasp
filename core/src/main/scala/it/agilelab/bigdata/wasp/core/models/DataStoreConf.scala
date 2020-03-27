package it.agilelab.bigdata.wasp.core.models

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._
import spray.json.{DefaultJsonProtocol, DeserializationException, JsObject, JsString, JsValue, RootJsonFormat, _}

sealed trait DataStoreConf {
  final val storage: String = this.getClass.getSimpleName
  val inputKeyColumn: String
}

final case class KeyValueDataStoreConf(inputKeyColumn: String,
                                       correlationIdColumn: String,
                                       keyValueModel: KeyValueModel, // here we retrieve tableName and namespace
                                       keyValueMatchingStrategy: KeyValueMatchingStrategy
                                      ) extends DataStoreConf

object KeyValueDataStoreConf {
  val TYPE = "keyValueDataStore"
}

final case class RawDataStoreConf(inputKeyColumn: String,
                                  correlationIdColumn: String,
                                  rawModel: RawModel, // here we retrieve location, schema, format and partitioning
                                  rawMatchingStrategy: RawMatchingStrategy,
                                  partitionPruningStrategy: PartitionPruningStrategy,
                                  missingPathFailure: Boolean = false // if true a missing path inside `rawModel` results in deletion failure
                                 ) extends DataStoreConf

object RawDataStoreConf {
  val TYPE = "rawDataStore"
}

sealed trait KeyValueMatchingStrategy

final case class ExactKeyValueMatchingStrategy() extends KeyValueMatchingStrategy

object ExactKeyValueMatchingStrategy {
  val TYPE = "exact"
}

final case class PrefixKeyValueMatchingStrategy() extends KeyValueMatchingStrategy

object PrefixKeyValueMatchingStrategy {
  val TYPE = "prefix"
}

final case class PrefixAndTimeBoundKeyValueMatchingStrategy(separator: String,
                                                            isDateFirst: Boolean,
                                                            pattern: String,
                                                            locale: String = "UTC") extends KeyValueMatchingStrategy

object PrefixAndTimeBoundKeyValueMatchingStrategy {
  val TYPE = "prefixAndTime"
}

sealed trait PartitionPruningStrategy

final case class TimeBasedBetweenPartitionPruningStrategy(colName: String, isDateNumeric: Boolean, pattern: String)
  extends PartitionPruningStrategy {
  def condition(start: Long, end: Long, zoneId: String): Column = {
    if (isDateNumeric) {
      rangeFilterFromMillisLongType(colName, pattern, start, end, zoneId)
    } else {
      rangeFilterFromStringType(colName, pattern, start, end, zoneId)
    }
  }

  private def rangeFilterFromMillisLongType(columnName: String, dateFormat: String, startTime: Long, endTime: Long, zoneId: String): Column = {
    unix_timestamp(to_utc_timestamp(to_timestamp(from_unixtime(col(columnName) / 1000)), zoneId))
      .between(lit(startTime / 1000), lit(endTime / 1000))
  }

  protected def rangeFilterFromStringType(columnName: String, dateFormat: String, startTime: Long, endTime: Long, zoneId: String): Column = {
    unix_timestamp(to_utc_timestamp(to_timestamp(col(columnName), dateFormat), zoneId))
      .between(lit(startTime / 1000), lit(endTime / 1000))
  }
}

object TimeBasedBetweenPartitionPruningStrategy {
  val TYPE = "timeBetweenPartition"
}

case class NoPartitionPruningStrategy() extends PartitionPruningStrategy

object NoPartitionPruningStrategy {
  val TYPE = "none"
}

sealed trait RawMatchingStrategy {
  val dataframeKeyMatchingExpression: String
}

case class ExactRawMatchingStrategy(dataframeKeyMatchingExpression: String) extends RawMatchingStrategy

object ExactRawMatchingStrategy {
  val TYPE = "exact"
}

case class PrefixRawMatchingStrategy(dataframeKeyMatchingExpression: String) extends RawMatchingStrategy

object PrefixRawMatchingStrategy {
  val TYPE = "prefix"
}

trait DataStoreConfJsonSupport extends DefaultJsonProtocol {

  def createDataStoreConfFormat: RootJsonFormat[DataStoreConf] = {
    // KeyValueDataStoreConf section
    implicit val exactKeyValueMatchingStrategyFormat: RootJsonFormat[ExactKeyValueMatchingStrategy] = jsonFormat0(ExactKeyValueMatchingStrategy.apply)
    implicit val prefixKeyValueMatchingStrategyFormat: RootJsonFormat[PrefixKeyValueMatchingStrategy] = jsonFormat0(PrefixKeyValueMatchingStrategy.apply)
    implicit val prefixAndTimeBoundKeyValueMatchingStrategyFormat: RootJsonFormat[PrefixAndTimeBoundKeyValueMatchingStrategy] = jsonFormat4(PrefixAndTimeBoundKeyValueMatchingStrategy.apply)
    implicit val keyValueMatchingStrategyFormat: RootJsonFormat[KeyValueMatchingStrategy] = new RootJsonFormat[KeyValueMatchingStrategy] {
      override def read(json: JsValue): KeyValueMatchingStrategy = {
        json.asJsObject.getFields("type") match {
          case Seq(JsString("ExactKeyValueMatchingStrategy")) => json.convertTo[ExactKeyValueMatchingStrategy]
          case Seq(JsString("PrefixKeyValueMatchingStrategy")) => json.convertTo[PrefixKeyValueMatchingStrategy]
          case Seq(JsString("PrefixAndTimeBoundKeyValueMatchingStrategy")) => json.convertTo[PrefixAndTimeBoundKeyValueMatchingStrategy]
          case _ => throw DeserializationException("Unknown json")
        }
      }

      override def write(obj: KeyValueMatchingStrategy): JsValue = JsObject((obj match {
        case e: ExactKeyValueMatchingStrategy => e.toJson
        case p: PrefixKeyValueMatchingStrategy => p.toJson
        case pt: PrefixAndTimeBoundKeyValueMatchingStrategy => pt.toJson
      }).asJsObject.fields + ("type" -> JsString(obj.getClass.getName)))
    }
    implicit val keyValueOptionFormat: RootJsonFormat[KeyValueOption] = jsonFormat2(KeyValueOption.apply)
    implicit val keyValueModelFormat: RootJsonFormat[KeyValueModel] = jsonFormat6(KeyValueModel.apply)
    implicit val keyValueDataStoreConfFormat: RootJsonFormat[KeyValueDataStoreConf] =
      jsonFormat(KeyValueDataStoreConf.apply, "inputKeyColumn", "correlationIdColumn", "keyValueModel", "keyMatchingStrategy")

    // RawDataStoreConf section
    implicit val timeBasedBetweenPartitionPruningStrategyFormat: RootJsonFormat[TimeBasedBetweenPartitionPruningStrategy] =
      jsonFormat(TimeBasedBetweenPartitionPruningStrategy.apply, "colName", "isDateNumeric", "pattern")
    implicit val partitionPruningFormat: RootJsonFormat[PartitionPruningStrategy] = new RootJsonFormat[PartitionPruningStrategy] {
      override def read(json: JsValue): PartitionPruningStrategy = json.asJsObject.getFields("type") match {
        case Seq(JsString("TimeBasedBetweenPartitionPruningStrategy")) => json.convertTo[TimeBasedBetweenPartitionPruningStrategy]
        case Seq(JsString("NoPartitionPruningStrategy")) => NoPartitionPruningStrategy()
        case _ => throw DeserializationException("Unknown json")
      }

      override def write(obj: PartitionPruningStrategy): JsValue = JsObject((obj match {
        case timeBased: TimeBasedBetweenPartitionPruningStrategy => timeBased.toJson
        case _: NoPartitionPruningStrategy => JsObject()
      }).asJsObject.fields + ("type" -> JsString(obj.getClass.getName)))
    }
    implicit val exactRawMatchingStrategyFormat: RootJsonFormat[ExactRawMatchingStrategy] = jsonFormat1(ExactRawMatchingStrategy.apply)
    implicit val prefixRawMatchingStrategyFormat: RootJsonFormat[PrefixRawMatchingStrategy] = jsonFormat1(PrefixRawMatchingStrategy.apply)
    implicit val rawMatchingStrategyFormat: RootJsonFormat[RawMatchingStrategy] = new RootJsonFormat[RawMatchingStrategy] {
      override def read(json: JsValue): RawMatchingStrategy = json.asJsObject.getFields("type") match {
        case Seq(JsString("ExactRawMatchingStrategy")) => json.convertTo[ExactRawMatchingStrategy]
        case Seq(JsString("PrefixRawMatchingStrategy")) => json.convertTo[PrefixRawMatchingStrategy]
        case _ => throw DeserializationException("Unknown json")
      }

      override def write(obj: RawMatchingStrategy): JsValue = JsObject((obj match {
        case exact: ExactRawMatchingStrategy => exact.toJson
        case prefix: PrefixRawMatchingStrategy => prefix.toJson
      }).asJsObject.fields + ("type" -> JsString(obj.getClass.getName)))
    }
    implicit val rawOptionsFormat: RootJsonFormat[RawOptions] = jsonFormat4(RawOptions.apply)
    implicit val rawModelFormat: RootJsonFormat[RawModel] = jsonFormat5(RawModel.apply)
    implicit val rawDataStoreConfFormat: RootJsonFormat[RawDataStoreConf] =
      jsonFormat(RawDataStoreConf.apply,
        "inputKeyColumn",
        "correlationIdColumn",
        "rawModel",
        "rawMatchingStrategy",
        "partitionPruningStrategy",
        "missingPathFailure")

    new RootJsonFormat[DataStoreConf] {
      override def read(json: JsValue): DataStoreConf = json.asJsObject.getFields("type") match {
        case Seq(JsString("KeyValueDataStoreConf")) => json.convertTo[KeyValueDataStoreConf]
        case Seq(JsString("RawDataStoreConf")) => json.convertTo[RawDataStoreConf]
        case _ => throw DeserializationException("Unknown json")
      }

      override def write(obj: DataStoreConf): JsValue = JsObject((obj match {
        case kv: KeyValueDataStoreConf => kv.toJson
        case raw: RawDataStoreConf => raw.toJson
      }).asJsObject.fields + ("type" -> JsString(obj.getClass.getName)))
    }

  }

}
