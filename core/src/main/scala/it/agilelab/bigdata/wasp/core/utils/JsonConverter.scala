package it.agilelab.bigdata.wasp.core.utils

import reactivemongo.bson._
import reactivemongo.bson.utils.Converters
import play.api.libs.json._
import play.api.libs.json.Json.toJsFieldJsValueWrapper
import scala.collection.Map
import scala.collection.immutable.Map
import scala.math.BigDecimal.double2bigDecimal
import scala.math.BigDecimal.int2bigDecimal
import scala.math.BigDecimal.long2bigDecimal

//TODO questo è preso da play reactive mongo Json

object BSONFormats {

  trait PartialFormat[T <: BSONValue] extends Format[T] {
    def partialReads: PartialFunction[JsValue, JsResult[T]]
    def partialWrites: PartialFunction[BSONValue, JsValue]

    def writes(t: T): JsValue = partialWrites(t)
    def reads(json: JsValue) = partialReads.lift(json).getOrElse(JsError(s"unhandled json value: $json"))
  }

  implicit object BSONDoubleFormat extends PartialFormat[BSONDouble] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONDouble]] = {
      case JsNumber(f)        => JsSuccess(BSONDouble(f.toDouble))
      case DoubleValue(value) => JsSuccess(BSONDouble(value))
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case double: BSONDouble => JsNumber(double.value)
    }

    private object DoubleValue {
      def unapply(obj: JsObject): Option[Double] =
        (obj \ "$double").asOpt[JsNumber].map(_.value.toDouble)
    }
  }

  implicit object BSONStringFormat extends PartialFormat[BSONString] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONString]] = {
      case JsString(str) => JsSuccess(BSONString(str))
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case str: BSONString => JsString(str.value)
    }
  }

  class BSONDocumentFormat(toBSON: JsValue => JsResult[BSONValue], toJSON: BSONValue => JsValue) extends PartialFormat[BSONDocument] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONDocument]] = {
      case obj: JsObject =>
        try {
          JsSuccess(bson(obj))
        } catch {
          case e: Throwable => JsError(e.getMessage)
        }
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case doc: BSONDocument => json(doc)
    }

    // UNSAFE - FOR INTERNAL USE
    private def bson(obj: JsObject): BSONDocument = BSONDocument(
      obj.fields.map { tuple =>
        tuple._1 -> (toBSON(tuple._2) match {
          case JsSuccess(bson, _) => bson
          case JsError(err)       => throw new RuntimeException(err.toString)
        })
      }
    )

    // UNSAFE - FOR INTERNAL USE
    private def json(bson: BSONDocument): JsObject =
      JsObject(bson.elements.map(elem => elem._1 -> toJSON(elem._2)))
  }

  implicit object BSONDocumentFormat extends BSONDocumentFormat(toBSON, toJSON)
  class BSONArrayFormat(toBSON: JsValue => JsResult[BSONValue], toJSON: BSONValue => JsValue) extends PartialFormat[BSONArray] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONArray]] = {
      case arr: JsArray =>
        try {
          JsSuccess(BSONArray(arr.value.map { value =>
            toBSON(value) match {
              case JsSuccess(bson, _) => bson
              case JsError(err)       => throw new RuntimeException(err.toString)
            }
          }))
        } catch {
          case e: Throwable => JsError(e.getMessage)
        }
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case array: BSONArray => JsArray(array.values.map(toJSON))
    }
  }

  implicit object BSONArrayFormat extends BSONArrayFormat(toBSON, toJSON)

  implicit object BSONObjectIDFormat extends PartialFormat[BSONObjectID] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONObjectID]] = {
      case OidValue(oid) => JsSuccess(BSONObjectID(oid))
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case oid: BSONObjectID => Json.obj("$oid" -> oid.stringify)
    }

    private object OidValue {
      def unapply(obj: JsObject): Option[String] =
        if (obj.fields.size != 1) None else (obj \ "$oid").asOpt[String]
    }
  }

  implicit object BSONJavaScriptFormat extends PartialFormat[BSONJavaScript] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONJavaScript]] = {
      case JavascriptValue(oid) => JsSuccess(BSONJavaScript(oid))
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case BSONJavaScript(code) => Json.obj("$javascript" -> code)
    }

    private object JavascriptValue {
      def unapply(obj: JsObject): Option[String] =
        if (obj.fields.size != 1) None else (obj \ "$javascript").asOpt[String]
    }
  }

  implicit object BSONBooleanFormat extends PartialFormat[BSONBoolean] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONBoolean]] = {
      case JsBoolean(v) => JsSuccess(BSONBoolean(v))
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case boolean: BSONBoolean => JsBoolean(boolean.value)
    }
  }

  implicit object BSONDateTimeFormat extends PartialFormat[BSONDateTime] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONDateTime]] = {
      case DateValue(value) => JsSuccess(BSONDateTime(value))
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case dt: BSONDateTime => Json.obj("$date" -> dt.value)
    }

    private object DateValue {
      def unapply(obj: JsObject): Option[Long] = (obj \ "$date").asOpt[Long]
    }
  }

  implicit object BSONTimestampFormat extends PartialFormat[BSONTimestamp] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONTimestamp]] = {
      case TimeValue((time, i)) => JsSuccess(BSONTimestamp(time, i))
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case ts: BSONTimestamp => Json.obj(
        "$time" -> (ts.value >>> 32), "$i" -> ts.value.toInt
      )
    }

    private object TimeValue {
      def unapply(obj: JsObject): Option[(Long, Int)] = for {
        time <- (obj \ "$time").asOpt[Long]
        i <- (obj \ "$i").asOpt[Int]
      } yield (time, i)
    }
  }

  implicit object BSONUndefinedFormat
    extends PartialFormat[BSONUndefined.type] {
    private object Undefined {
      def unapply(obj: JsObject): Option[BSONUndefined.type] =
        obj.value.get("$undefined") match {
          case Some(JsBoolean(true)) => Some(BSONUndefined)
          case _                     => None
        }
    }

    val partialReads: PartialFunction[JsValue, JsResult[BSONUndefined.type]] = {
      case Undefined(bson) => JsSuccess(bson)
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case BSONUndefined => Json.obj("$undefined" -> true)
    }
  }

  implicit object BSONRegexFormat extends PartialFormat[BSONRegex] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONRegex]] = {
      case js: JsObject if js.values.size == 1 && js.fields.head._1 == "$regex" =>
        js.fields.head._2.asOpt[String].
          map(rx => JsSuccess(BSONRegex(rx, ""))).
          getOrElse(JsError(__ \ "$regex", "string expected"))
      case js: JsObject if js.value.size == 2 && js.value.exists(_._1 == "$regex") && js.value.exists(_._1 == "$options") =>
        val rx = (js \ "$regex").asOpt[String]
        val opts = (js \ "$options").asOpt[String]
        (rx, opts) match {
          case (Some(rx), Some(opts)) => JsSuccess(BSONRegex(rx, opts))
          case (None, Some(_))        => JsError(__ \ "$regex", "string expected")
          case (Some(_), None)        => JsError(__ \ "$options", "string expected")
          case _                      => JsError(__ \ "$regex", "string expected") ++ JsError(__ \ "$options", "string expected")
        }
    }
    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case rx: BSONRegex =>
        if (rx.flags.isEmpty)
          Json.obj("$regex" -> rx.value)
        else Json.obj("$regex" -> rx.value, "$options" -> rx.flags)
    }
  }

  implicit object BSONNullFormat extends PartialFormat[BSONNull.type] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONNull.type]] = {
      case JsNull => JsSuccess(BSONNull)
    }
    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case BSONNull => JsNull
    }
  }

  implicit object BSONIntegerFormat extends PartialFormat[BSONInteger] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONInteger]] = {
      case JsNumber(i)     => JsSuccess(BSONInteger(i.toInt))
      case IntValue(value) => JsSuccess(BSONInteger(value))
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case int: BSONInteger => JsNumber(int.value)
    }

    private object IntValue {
      def unapply(obj: JsObject): Option[Int] =
        (obj \ "$int").asOpt[JsNumber].map(_.value.toInt)
    }
  }

  implicit object BSONLongFormat extends PartialFormat[BSONLong] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONLong]] = {
      case JsNumber(long)   => JsSuccess(BSONLong(long.toLong))
      case LongValue(value) => JsSuccess(BSONLong(value))
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case long: BSONLong => JsNumber(long.value)
    }

    private object LongValue {
      def unapply(obj: JsObject): Option[Long] =
        (obj \ "$long").asOpt[JsNumber].map(_.value.toLong)
    }
  }

  implicit object BSONBinaryFormat extends PartialFormat[BSONBinary] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONBinary]] = {
      case JsString(str) => try {
        JsSuccess(BSONBinary(Converters.str2Hex(str), Subtype.UserDefinedSubtype))
      } catch {
        case e: Throwable => JsError(s"error deserializing hex ${e.getMessage}")
      }
      case obj: JsObject if obj.fields.exists {
        case (str, _: JsString) if str == "$binary" => true
        case _                                      => false
      } => try {
        JsSuccess(BSONBinary(Converters.str2Hex((obj \ "$binary").as[String]), Subtype.UserDefinedSubtype))
      } catch {
        case e: Throwable => JsError(s"error deserializing hex ${e.getMessage}")
      }
    }
    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case binary: BSONBinary =>
        val remaining = binary.value.readable()
        Json.obj(
          "$binary" -> Converters.hex2Str(binary.value.slice(remaining).readArray(remaining)),
          "$type" -> Converters.hex2Str(Array(binary.subtype.value.toByte))
        )
    }
  }

  implicit object BSONSymbolFormat extends PartialFormat[BSONSymbol] {
    val partialReads: PartialFunction[JsValue, JsResult[BSONSymbol]] = {
      case SymbolValue(value) => JsSuccess(BSONSymbol(value))
    }

    val partialWrites: PartialFunction[BSONValue, JsValue] = {
      case BSONSymbol(s) => Json.obj("$symbol" -> s)
    }

    private object SymbolValue {
      def unapply(obj: JsObject): Option[String] =
        if (obj.fields.size != 1) None else (obj \ "$symbol").asOpt[String]
    }
  }

  val numberReads: PartialFunction[JsValue, JsResult[BSONValue]] = {
    case JsNumber(n) if !n.ulp.isWhole => JsSuccess(BSONDouble(n.toDouble))
    case JsNumber(n) if n.isValidInt   => JsSuccess(BSONInteger(n.toInt))
    case JsNumber(n)                   => JsSuccess(BSONLong(n.toLong))
  }

  def toBSON(json: JsValue): JsResult[BSONValue] =
    BSONStringFormat.partialReads.
      orElse(BSONObjectIDFormat.partialReads).
      orElse(BSONJavaScriptFormat.partialReads).
      orElse(BSONDateTimeFormat.partialReads).
      orElse(BSONTimestampFormat.partialReads).
      orElse(BSONBinaryFormat.partialReads).
      orElse(BSONRegexFormat.partialReads).
      orElse(numberReads).
      orElse(BSONBooleanFormat.partialReads).
      orElse(BSONNullFormat.partialReads).
      orElse(BSONSymbolFormat.partialReads).
      orElse(BSONArrayFormat.partialReads).
      orElse(BSONDocumentFormat.partialReads).
      orElse(BSONUndefinedFormat.partialReads).
      lift(json).getOrElse(JsError(s"unhandled json value: $json"))

  def toJSON(bson: BSONValue): JsValue = BSONObjectIDFormat.partialWrites.
    orElse(BSONJavaScriptFormat.partialWrites).
    orElse(BSONDateTimeFormat.partialWrites).
    orElse(BSONTimestampFormat.partialWrites).
    orElse(BSONBinaryFormat.partialWrites).
    orElse(BSONRegexFormat.partialWrites).
    orElse(BSONDoubleFormat.partialWrites).
    orElse(BSONIntegerFormat.partialWrites).
    orElse(BSONLongFormat.partialWrites).
    orElse(BSONBooleanFormat.partialWrites).
    orElse(BSONNullFormat.partialWrites).
    orElse(BSONStringFormat.partialWrites).
    orElse(BSONSymbolFormat.partialWrites).
    orElse(BSONArrayFormat.partialWrites).
    orElse(BSONDocumentFormat.partialWrites).
    orElse(BSONUndefinedFormat.partialWrites).
    lift(bson).getOrElse(throw new RuntimeException(s"Unhandled json value: $bson"))


  def toString(doc: BSONValue): String = {
    toJSON(doc).toString()
  }

  def fromString(doc: String): Option[BSONDocument] = {
    toBSON(Json.parse(doc)).asOpt.flatMap {
      case bsonDocument: BSONDocument => Some(bsonDocument)
      case _ => None
    }
  }


}



object Writers {
  implicit class JsPathMongo(val jp: JsPath) extends AnyVal {
    def writemongo[A](implicit writer: Writes[A]): OWrites[A] = {
      OWrites[A] { (o: A) =>
        val newPath = jp.path.flatMap {
          case e: KeyPathNode     => Some(e.key)
          case e: RecursiveSearch => Some(s"$$.${e.key}")
          case e: IdxPathNode     => Some(s"${e.idx}")
        }.mkString(".")

        val orig = writer.writes(o)
        orig match {
          case JsObject(e) =>
            JsObject(e.flatMap {
              case (k, v) => Seq(s"${newPath}.$k" -> v)
            })
          case e: JsValue => JsObject(Seq(newPath -> e))
        }
      }
    }
  }
}