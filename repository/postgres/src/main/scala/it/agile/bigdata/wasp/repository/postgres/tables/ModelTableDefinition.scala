package it.agile.bigdata.wasp.repository.postgres.tables

import it.agilelab.bigdata.wasp.models.Model


trait ModelTableDefinition[T<:Model] extends TableDefinition[T,String]{

  protected val name = "name"
  override lazy val conditionPrimaryKey : String => String = k=> s"$name='$k'"
  override lazy val primaryKeyFromObject : T => String = obj => obj.name


}
