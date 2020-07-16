package it.agile.bigdata.wasp.repository.postgres

import it.agile.bigdata.wasp.repository.postgres.bl.PostgresFactoryBL
import it.agilelab.bigdata.wasp.repository.core.bl.FactoryBL
import it.agilelab.bigdata.wasp.repository.core.db.{RepositoriesFactory, WaspDB}

class PostgresRepositoryFactory extends RepositoriesFactory {

  override def getDB(): WaspDB = WaspPostgresDB.getDB()

  override def initializeDB(): WaspDB = WaspPostgresDB.initializeDB()

  override def dropDatabase(): Unit = {}

  override def getFactoryBL: FactoryBL = new PostgresFactoryBL

}
