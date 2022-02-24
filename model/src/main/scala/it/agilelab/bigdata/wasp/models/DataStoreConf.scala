package it.agilelab.bigdata.wasp.models

import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoUnit, TemporalUnit}
import java.time.{Instant, ZoneId, ZonedDateTime}

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
                                                            pattern: String,
                                                            locale: String = "UTC") extends KeyValueMatchingStrategy

object PrefixAndTimeBoundKeyValueMatchingStrategy {
  val TYPE = "prefixAndTime"
}

sealed trait PartitionPruningStrategy

final case class TimeBasedBetweenPartitionPruningStrategy(columnName: String,
                                                          isDateNumeric: Boolean,
                                                          pattern: String,
                                                          granularity: String)
  extends PartitionPruningStrategy {
  def condition(start: Long, end: Long, zoneId: String): Column = {
    if (isDateNumeric) {
      rangeFilterFromMillisLongType(start, end, zoneId)
    } else {
      rangeFilterFromStringType(start, end, zoneId)
    }
  }

  private def rangeFilterFromStringType(startTime: Long, endTime: Long, zoneId: String): Column = {
    val dateFormatter = DateTimeFormatter.ofPattern(pattern)
    val startRefDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(startTime), ZoneId.of(zoneId))
    val endRefDate = ZonedDateTime.ofInstant(Instant.ofEpochMilli(endTime), ZoneId.of(zoneId))
    val granularityUnit = ChronoUnit.valueOf(granularity)
    val datesToConsider = getDatesToConsider(startRefDate, endRefDate, dateFormatter, granularityUnit)
    col(columnName).isin(datesToConsider: _*)
  }

  private def rangeFilterFromMillisLongType(startTime: Long, endTime: Long, zoneId: String): Column = {
    unix_timestamp(to_utc_timestamp(to_timestamp(from_unixtime(col(columnName) / 1000)), zoneId))
      .between(lit(startTime / 1000), lit(endTime / 1000))
  }

  @scala.annotation.tailrec
  private def getDatesToConsider(startRefDate: ZonedDateTime,
                                 endRefDate: ZonedDateTime,
                                 dateFormatter: DateTimeFormatter,
                                 granularity: TemporalUnit,
                                 dateToConsider: List[String] = Nil): List[String] = {
    if (startRefDate.isAfter(endRefDate)) {
      dateToConsider.reverse
    } else {
      getDatesToConsider(
        startRefDate.plus(1, granularity),
        endRefDate,
        dateFormatter,
        granularity,
        dateFormatter.format(startRefDate) :: dateToConsider
      )
    }
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

case class ContainsRawMatchingStrategy(dataframeKeyMatchingExpression: String) extends RawMatchingStrategy

object ContainsRawMatchingStrategy {
  val TYPE = "contains"
}

trait DataStoreConfJsonSupport extends DefaultJsonProtocol {

  def createDataStoreConfFormat: RootJsonFormat[DataStoreConf] = {
    // KeyValueDataStoreConf section
    implicit val exactKeyValueMatchingStrategyFormat: RootJsonFormat[ExactKeyValueMatchingStrategy] = jsonFormat0(ExactKeyValueMatchingStrategy.apply)
    implicit val prefixKeyValueMatchingStrategyFormat: RootJsonFormat[PrefixKeyValueMatchingStrategy] = jsonFormat0(PrefixKeyValueMatchingStrategy.apply)
    implicit val prefixAndTimeBoundKeyValueMatchingStrategyFormat: RootJsonFormat[PrefixAndTimeBoundKeyValueMatchingStrategy] = jsonFormat3(PrefixAndTimeBoundKeyValueMatchingStrategy.apply)
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
      }).asJsObject.fields + ("type" -> JsString(obj.getClass.getSimpleName)))
    }
    implicit val keyValueOptionFormat: RootJsonFormat[KeyValueOption] = jsonFormat2(KeyValueOption.apply)
    implicit val keyValueModelFormat: RootJsonFormat[KeyValueModel] = jsonFormat6(KeyValueModel.apply)
    implicit val keyValueDataStoreConfFormat: RootJsonFormat[KeyValueDataStoreConf] =
      jsonFormat(KeyValueDataStoreConf.apply, "inputKeyColumn", "correlationIdColumn", "keyValueModel", "keyMatchingStrategy")

    // RawDataStoreConf section
    implicit val timeBasedBetweenPartitionPruningStrategyFormat: RootJsonFormat[TimeBasedBetweenPartitionPruningStrategy] =
      jsonFormat(TimeBasedBetweenPartitionPruningStrategy.apply, "columnName", "isDateNumeric", "pattern", "granularity")
    implicit val partitionPruningFormat: RootJsonFormat[PartitionPruningStrategy] = new RootJsonFormat[PartitionPruningStrategy] {
      override def read(json: JsValue): PartitionPruningStrategy = json.asJsObject.getFields("type") match {
        case Seq(JsString("TimeBasedBetweenPartitionPruningStrategy")) => json.convertTo[TimeBasedBetweenPartitionPruningStrategy]
        case Seq(JsString("NoPartitionPruningStrategy")) => NoPartitionPruningStrategy()
        case _ => throw DeserializationException("Unknown json")
      }

      override def write(obj: PartitionPruningStrategy): JsValue = JsObject((obj match {
        case timeBased: TimeBasedBetweenPartitionPruningStrategy => timeBased.toJson
        case _: NoPartitionPruningStrategy => JsObject()
      }).asJsObject.fields + ("type" -> JsString(obj.getClass.getSimpleName)))
    }
    implicit val exactRawMatchingStrategyFormat: RootJsonFormat[ExactRawMatchingStrategy] = jsonFormat1(ExactRawMatchingStrategy.apply)
    implicit val prefixRawMatchingStrategyFormat: RootJsonFormat[PrefixRawMatchingStrategy] = jsonFormat1(PrefixRawMatchingStrategy.apply)
    implicit val containsRawMatchingStrategyFormat: RootJsonFormat[ContainsRawMatchingStrategy] = jsonFormat1(ContainsRawMatchingStrategy.apply)
    implicit val rawMatchingStrategyFormat: RootJsonFormat[RawMatchingStrategy] = new RootJsonFormat[RawMatchingStrategy] {
      override def read(json: JsValue): RawMatchingStrategy = json.asJsObject.getFields("type") match {
        case Seq(JsString("ExactRawMatchingStrategy")) => json.convertTo[ExactRawMatchingStrategy]
        case Seq(JsString("PrefixRawMatchingStrategy")) => json.convertTo[PrefixRawMatchingStrategy]
        case Seq(JsString("ContainsRawMatchingStrategy")) => json.convertTo[ContainsRawMatchingStrategy]
        case _ => throw DeserializationException("Unknown json")
      }

      override def write(obj: RawMatchingStrategy): JsValue = JsObject((obj match {
        case exact: ExactRawMatchingStrategy => exact.toJson
        case prefix: PrefixRawMatchingStrategy => prefix.toJson
        case contains: ContainsRawMatchingStrategy => contains.toJson
      }).asJsObject.fields + ("type" -> JsString(obj.getClass.getSimpleName)))
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
      }).asJsObject.fields + ("type" -> JsString(obj.getClass.getSimpleName)))
    }

  }

}
