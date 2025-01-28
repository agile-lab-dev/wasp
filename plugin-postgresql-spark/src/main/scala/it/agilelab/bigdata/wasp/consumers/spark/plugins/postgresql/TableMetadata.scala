package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

class TableMetadata(name: String, columnsMetadata: Seq[ColumnMetadata]) extends Serializable {
  private[this] lazy val _columnNameToMetadata: Map[String, ColumnMetadata] =
    columnsMetadata.map(cm => cm.name -> cm).toMap
  private[this] lazy val _columnIndexToMetadata: Map[Int, ColumnMetadata] =
    columnsMetadata.map(cm => cm.index -> cm).toMap

  def columnNameToMetadata: Map[String, ColumnMetadata] = _columnNameToMetadata
  def columnIndexToMetadata: Map[Int, ColumnMetadata]   = _columnIndexToMetadata
}

case class ColumnMetadata(name: String, index: Int, typeNumber: Int, typeName: String, nullable: Boolean)
