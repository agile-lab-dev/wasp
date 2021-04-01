package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.models.DatastoreModel
import it.agilelab.bigdata.wasp.repository.core.bl.TopicBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{TableDefinition, TopicTableDefinition}

case class TopicBLImpl(waspDB : WaspPostgresDB) extends TopicBL with PostgresBL{

  override implicit val tableDefinition: TableDefinition[DatastoreModel, String] = TopicTableDefinition

  override def getByName(name: String): Option[DatastoreModel] = waspDB.getByPrimaryKey(name)

  override def getAll: Seq[DatastoreModel] = waspDB.getAll()

  override def persist(topicModel: DatastoreModel): Unit = waspDB.insert(topicModel)

  override def upsert(topicModel: DatastoreModel): Unit = waspDB.upsert(topicModel)

  override def insertIfNotExists(topicDatastoreModel: DatastoreModel): Unit = waspDB.insertIfNotExists(topicDatastoreModel)

}
