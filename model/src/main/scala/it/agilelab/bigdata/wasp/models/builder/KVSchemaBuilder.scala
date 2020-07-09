package it.agilelab.bigdata.wasp.models.builder

import it.agilelab.bigdata.wasp.models.builder.KVSchemaBuilder.{CompleteKVSchema, KVSchema, KeyField, WithColumn, WithKey}

import scala.language.implicitConversions

object KVSchemaBuilder {

  sealed trait KVSchema

  sealed trait EmptyKVSchema extends KVSchema

  sealed trait WithKey extends KVSchema

  sealed trait WithColumn extends KVSchema

  type CompleteKVSchema = EmptyKVSchema with WithKey with WithColumn

  val emptySchemaBuilder = KVSchemaBuilder.apply[EmptyKVSchema]()

  case class KeyField(name: String, `type`: KVType)

  implicit def Tuple2toKeyField(x: (String, KVType)): KeyField = KeyField(x._1, x._2)

}

case class KVSchemaBuilder[ThisKVSchema <: KVSchema](keyField: Option[KeyField] = None,
                                                     columns: Seq[KVColumnFamily] = Seq.empty) {
  def build[S <: KVSchema](implicit ev: ThisKVSchema =:= CompleteKVSchema): String = {
    buildSeq.mkString(",\n")
  }

  def buildSeq[S <: KVSchema](implicit ev: ThisKVSchema =:= CompleteKVSchema): Seq[String] = {
    checkDuplicateColumns(columns)

    val keyJson = s""""${keyField.get.name}": {"cf": "rowkey", "col": "key", "type": "${keyField.get.`type`.name}"}"""

    val grouped = columns.zipWithIndex.flatMap { case (cf, index) =>
      cf.toJson.map { line =>
        if (line.startsWith("\"clustering\"")) {
          line.replaceAllLiterally("clustering", s"clustering_$index")
        } else {
          line
        }
      }
    }.foldLeft(List.empty[(String, String)]) { (z, x) =>
      val k = x.split("\": ", 2)(0).drop(1)
      if (z.exists(_._1 == k)) {
        z
      } else {
        (k, x) :: z
      }
    }.reverse

    val cfLines = grouped.map(_._2)
    keyJson +: cfLines
  }


  def withKey(keyField: KeyField): KVSchemaBuilder[ThisKVSchema with WithKey] =
    new KVSchemaBuilder(Some(keyField), columns)

  def withFamily(c: KVColumnFamily): KVSchemaBuilder[ThisKVSchema with WithColumn] =
    new KVSchemaBuilder(keyField, columns :+ c)

  private def checkDuplicateColumns(columnFamilies: Seq[KVColumnFamily]): Unit = {
    columnFamilies.groupBy(_.name).foreach { case (q, elements) =>
      if (elements.length > 1) {
        throw new IllegalStateException(s"Cannot build the schema because column family $q is declared multiple times")
      }
    }
  }

}
