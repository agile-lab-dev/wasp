package it.agilelab.bigdata.wasp.models.builder

import it.agilelab.bigdata.wasp.models.builder.KVColumnFamily.ColumnFamilyBuilder.{ColumnFamilyBuilderState, CompleteColumnFamilyBuilderState, WithCellQualifierColumnFamilyBuilderState, WithNameColumnFamilyBuilderState}

sealed abstract class KVColumnFamily(val name: String,
                                     val cellQualifiers: Seq[KVColumn]) {
  def toJson: Seq[String]
}

object KVColumnFamily {

  private final case class SimpleKVColumnFamily(override val name: String,
                                                override val cellQualifiers: Seq[KVColumn])
    extends KVColumnFamily(name, cellQualifiers) {
    override def toJson: Seq[String] = {
      cellQualifiers.map(_.toJson(name))
    }
  }

  private final case class ClusteredKVColumnFamily(override val name: String,
                                                   clusteringColumns: Seq[PrimitiveKVColumn],
                                                   override val cellQualifiers: Seq[KVColumn])
    extends KVColumnFamily(name, cellQualifiers) {
    override def toJson: Seq[String] = {
      val columns = clusteringColumns.map(_.qualifier).mkString(":")
      s""""clustering": {"cf": "$name", "columns": "$columns"}""" +:
        (clusteringColumns ++ cellQualifiers).map(_.toJson(name))
    }
  }

  // region Builder

  object ColumnFamilyBuilder {

    sealed trait ColumnFamilyBuilderState

    sealed trait EmptyColumnFamilyBuilderState extends ColumnFamilyBuilderState

    sealed trait WithNameColumnFamilyBuilderState extends ColumnFamilyBuilderState

    sealed trait WithCellQualifierColumnFamilyBuilderState extends ColumnFamilyBuilderState

    type CompleteColumnFamilyBuilderState =
      EmptyColumnFamilyBuilderState with WithNameColumnFamilyBuilderState with WithCellQualifierColumnFamilyBuilderState

    val emptyColumnFamilyBuilder: ColumnFamilyBuilder[EmptyColumnFamilyBuilderState] =
      ColumnFamilyBuilderImpl[EmptyColumnFamilyBuilderState]()

    def withName(name: String) = emptyColumnFamilyBuilder.withName(name)

  }

  trait ColumnFamilyBuilder[CurrentState <: ColumnFamilyBuilderState] {

    def withName(name: String): ColumnFamilyBuilder[CurrentState with WithNameColumnFamilyBuilderState]

    def withCellQualifier(cq: KVColumn): ColumnFamilyBuilder[CurrentState with WithCellQualifierColumnFamilyBuilderState]

    def withClusteringColumn(cq: PrimitiveKVColumn): ColumnFamilyBuilder[CurrentState]

    def build(implicit ev: CurrentState =:= CompleteColumnFamilyBuilderState): KVColumnFamily
  }


  private case class ColumnFamilyBuilderImpl[CurrentState <: ColumnFamilyBuilderState](name: Option[String] = None,
                                                                                       cellQualifiers: Seq[KVColumn] = Seq.empty,
                                                                                       clusteringColumns: Seq[PrimitiveKVColumn] = Seq.empty) extends ColumnFamilyBuilder[CurrentState] {
    override def withName(name: String): ColumnFamilyBuilder[CurrentState with WithNameColumnFamilyBuilderState] =
      this.copy(name = Some(name))

    override def withCellQualifier(cq: KVColumn): ColumnFamilyBuilder[CurrentState with WithCellQualifierColumnFamilyBuilderState] =
      this.copy(cellQualifiers = cellQualifiers :+ cq)

    override def withClusteringColumn(cq: PrimitiveKVColumn): ColumnFamilyBuilder[CurrentState] =
      this.copy(clusteringColumns = clusteringColumns :+ cq)

    override def build(implicit ev: CurrentState =:= CompleteColumnFamilyBuilderState): KVColumnFamily = {
      checkDuplicateColumns(clusteringColumns)
      checkDuplicateColumns(cellQualifiers)
      if (clusteringColumns.isEmpty) {
        KVColumnFamily.SimpleKVColumnFamily(name.get, cellQualifiers)
      } else {
        KVColumnFamily.ClusteredKVColumnFamily(name.get, clusteringColumns, cellQualifiers)
      }
    }

    private def checkDuplicateColumns(cellQualifiers: Seq[KVColumn]): Unit = {
      cellQualifiers.groupBy(_.qualifier).foreach { case (q, elements) =>
        if (elements.length > 1) {
          throw new IllegalStateException(s"Cannot build Column Family ${name.get} because multiple fields mapped " +
            s"to the same qualifier [$q]")
        }
      }
    }
  }

  // endregion

}
