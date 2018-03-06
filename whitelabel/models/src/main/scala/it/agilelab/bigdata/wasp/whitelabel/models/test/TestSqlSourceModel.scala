package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.core.models._

private[wasp] object TestSqlSouceModel {

  lazy val mySql = SqlSourceModel(
    name = "TestMySqlModel",
    connectionName = "mysql", // have to be present in jdbc-subConfig
    database = "test_db",     // just useful to use it in Strategy - not used to create the url (JDBC connection string) which is retrieved from jdbc-subConfig
    dbtable = "test_table",
    partitioningInfo = None,
    numPartitions = None,
    fetchSize = None
  )
}