package it.agilelab.bigdata.wasp.core.models.builder

import scala.annotation.tailrec
import scala.reflect.macros.blackbox

sealed abstract class KVColumn(val fieldName: String, val qualifier: String) {
  def toJson(cfName: String): String
}

object KVColumn {

  // this can be made better to check that obj.fieldName is a Product or Option[Product]
  def avro(fieldName: String,
           qualifier: String,
           avro: String): AvroKVColumn = AvroKVColumn(fieldName, qualifier, avro)

  // this can be made better to check that obj.fieldName is of type KVType or Option[KVType]
  def primitive(fieldName: String,
                qualifier: String,
                primitive: KVType): PrimitiveKVColumn = PrimitiveKVColumn(fieldName, qualifier, primitive)

  def primitive(fieldName: String,
                qualifier: String,
                primitive: String): PrimitiveKVColumn = {
    KVType.get(primitive).map(PrimitiveKVColumn(fieldName, qualifier, _))
      .getOrElse(throw new IllegalArgumentException(s"Unsupported type $primitive"))
  }

  def primitive[T, V](expr: T => V, qualifier: String): PrimitiveKVColumn = macro KVColumnMacros.primitiveImpl[V]
}

final case class AvroKVColumn(override val fieldName: String,
                              override val qualifier: String,
                              avro: String) extends KVColumn(fieldName, qualifier) {
  override def toJson(cfName: String): String = {
    s""""$fieldName": {"cf": "$cfName", "col": "$qualifier", "avro": "$avro"}"""
  }
}

final case class PrimitiveKVColumn(override val fieldName: String,
                                   override val qualifier: String,
                                   primitive: KVType) extends KVColumn(fieldName, qualifier) {
  override def toJson(cfName: String): String = {
    s""""$fieldName": {"cf": "$cfName", "col": "$qualifier", "type": "${primitive.name}"}"""
  }
}

object KVColumnMacros {
  def primitiveImpl[T](c: blackbox.Context)(expr: c.Expr[T], qualifier: c.Expr[String])(implicit tag: c.WeakTypeTag[T]): c.Expr[PrimitiveKVColumn] = {
    import c.universe._

    @tailrec def extract(tree: c.Tree): c.Name = tree match {
      case Ident(n) => n
      case Select(_, n) => n
      case Function(_, body) => extract(body)
      case Block(_, exp) => extract(exp)
      case Apply(func, _) => extract(func)
      case TypeApply(func, _) => extract(func)
      case _ => c.abort(c.enclosingPosition, s"Unsupported expression: $expr")
    }

    @tailrec def extractType(tpe: c.universe.Type, isParentArray: Boolean, isParentOption: Boolean): String = {
      showRaw(tpe.typeSymbol.name).toLowerCase match {
        case "option" =>
          if (isParentOption) {
            "Option[Option[_]]"
          } else if (isParentArray) {
            "Option[Array[_]]"
          } else {
            extractType(tpe.typeArgs.head, isParentArray = false, isParentOption = true)
          }
        case "array" =>
          if (isParentArray) {
            "Array[Array[_]]"
          } else {
            extractType(tpe.typeArgs.head, isParentArray = true, isParentOption = false)
          }
        case "byte" | "scala.byte" =>
          if (isParentArray) {
            "bytes"
          } else {
            "byte"
          }
        case a =>
          if (isParentArray) {
            s"Array[$a]"
          } else {
            a
          }
      }
    }

    val name = extract(expr.tree).decodedName.toString
    val typeName = extractType(tag.tpe, isParentArray = false, isParentOption = false)
    KVType.get(typeName).getOrElse(c.abort(c.enclosingPosition, s"Unsupported type: $typeName"))
    reify {
      KVColumn.primitive(
        c.Expr[String] {
          Literal(Constant(name))
        }.splice,
        qualifier.splice,
        c.Expr[String] {
          Literal(Constant(typeName))
        }.splice
      )
    }
  }
}
