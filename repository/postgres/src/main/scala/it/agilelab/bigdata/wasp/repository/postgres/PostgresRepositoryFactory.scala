package it.agilelab.bigdata.wasp.repository.postgres

import it.agilelab.bigdata.wasp.repository.core.bl.FactoryBL
import it.agilelab.bigdata.wasp.repository.core.db.{RepositoriesFactory, WaspDB}
import it.agilelab.bigdata.wasp.repository.postgres.bl.PostgresFactoryBL

class PostgresRepositoryFactory extends RepositoriesFactory {

  override def getDB(): WaspDB = WaspPostgresDB.getDB()

  override def initializeDB(): WaspDB = WaspPostgresDB.initializeDB()

  override def dropDatabase(): Unit = {}

  override def getFactoryBL: FactoryBL = new PostgresFactoryBL

}
