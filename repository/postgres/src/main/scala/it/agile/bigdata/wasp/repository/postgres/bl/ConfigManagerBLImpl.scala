package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agile.bigdata.wasp.repository.postgres.tables.{ConfigManagerTableDefinition, TableDefinition}
import it.agilelab.bigdata.wasp.models.Model
import it.agilelab.bigdata.wasp.repository.core.bl.ConfigManagerBL

import scala.reflect.ClassTag
import scala.reflect.runtime.universe


case class ConfigManagerBLImpl(waspDB: WaspPostgresDB) extends ConfigManagerBL with PostgresBL {

  override implicit val tableDefinition: TableDefinition[Model,String] = ConfigManagerTableDefinition

  override def getByName[T <: Model](name: String)(implicit ct: ClassTag[T], typeTag: universe.TypeTag[T]): Option[T] = {
    waspDB.getByPrimaryKey(name).map(_.asInstanceOf[T])
  }

  override def retrieveConf[T <: Model](default: T, nameConf: String)(implicit ct: ClassTag[T], typeTag: universe.TypeTag[T]): Option[T] = {
    waspDB.insertIfNotExists(default:Model)
    getByName[T](nameConf)
  }

  override def retrieveDBConfig(): Seq[String] =
    waspDB.selectAll(tableDefinition.tableName,Array(ConfigManagerTableDefinition.getPayload),None)(rs => rs.getString(ConfigManagerTableDefinition.getPayload))
}
