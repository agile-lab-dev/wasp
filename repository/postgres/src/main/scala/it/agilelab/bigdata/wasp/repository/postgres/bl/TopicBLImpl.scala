package it.agilelab.bigdata.wasp.repository.postgres.bl

import it.agilelab.bigdata.wasp.repository.postgres.tables.TopicTableDefinition
import it.agilelab.bigdata.wasp.datastores.TopicCategory
import it.agilelab.bigdata.wasp.models.DatastoreModel
import it.agilelab.bigdata.wasp.repository.core.bl.TopicBL
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agilelab.bigdata.wasp.repository.postgres.tables.{TableDefinition, TopicTableDefinition}

case class TopicBLImpl(waspDB : WaspPostgresDB) extends TopicBL with PostgresBL{

  override implicit val tableDefinition: TableDefinition[DatastoreModel[TopicCategory], String] = TopicTableDefinition

  override def getByName(name: String): Option[DatastoreModel[TopicCategory]] = waspDB.getByPrimaryKey(name)

  override def getAll: Seq[DatastoreModel[TopicCategory]] = waspDB.getAll()

  override def persist(topicModel: DatastoreModel[TopicCategory]): Unit = waspDB.insert(topicModel)

  override def upsert(topicModel: DatastoreModel[TopicCategory]): Unit = waspDB.upsert(topicModel)

  override def insertIfNotExists(topicDatastoreModel: DatastoreModel[TopicCategory]): Unit = waspDB.insertIfNotExists(topicDatastoreModel)

}
