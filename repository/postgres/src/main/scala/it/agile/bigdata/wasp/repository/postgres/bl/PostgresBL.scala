package it.agile.bigdata.wasp.repository.postgres.bl

import it.agile.bigdata.wasp.repository.postgres.WaspPostgresDB
import it.agile.bigdata.wasp.repository.postgres.tables.TableDefinition

trait PostgresBL {

   val waspDB: WaspPostgresDB

   implicit val tableDefinition: TableDefinition[_,_]

   def createTable(): Unit = {
      waspDB.createTable()
   }

   private[postgres] def dropTable() : Unit = {
      waspDB.dropTable()
   }

}
