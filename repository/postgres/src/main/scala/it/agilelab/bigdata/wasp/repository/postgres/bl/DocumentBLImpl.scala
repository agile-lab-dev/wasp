package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.repository.postgres.tables.DocumentTableDefinition
import it.agilelab.bigdata.wasp.models.DocumentModel
import it.agilelab.bigdata.wasp.repository.core.bl.DocumentBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{DocumentTableDefinition, TableDefinition}

case class DocumentBLImpl(waspDB: WaspPostgresDB ) extends DocumentBL with PostgresBL {

  implicit val tableDefinition: TableDefinition[DocumentModel,String] = DocumentTableDefinition


  override def getByName(name: String): Option[DocumentModel] = waspDB.getByPrimaryKey(name)

  override def getAll(): Seq[DocumentModel] = waspDB.getAll()

  override def persist(document: DocumentModel): Unit = waspDB.insert(document)

  override def upsert(document: DocumentModel): Unit = waspDB.upsert(document)

  override def createTable(): Unit = waspDB.createTable()

}
