package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import it.agilelab.bigdata.wasp.core.logging.Logging

import java.sql.{Connection, Timestamp}

trait PostgresTestSupport extends Logging {

  /**
    * Squashes a sequence of [[TestData]] according to the primary key (pk1, pk2) using these rules:
    * "val1" => "case when existing.val1 is null then excluded.val1 else existing.val1 end"
    * "val2" => "case when existing.val2 is null then excluded.val2 else least(existing.val2, excluded.val2) end"
    * "val3" => "case when existing.val3 is null then excluded.val3 else greatest(existing.val3, excluded.val3) end"
    *
    * @param testData sequence with the TestData to squash
    * @return a sequence with the squashed TestData
    */
  def squash(testData: Seq[TestData]): Seq[TestData] = {
    testData
      .groupBy(x => (x.pk1, x.pk2))
      .map {
        case (_, values) =>
          values.reduce[TestData] {
            case (x, y) =>
              TestData(
                x.pk1,
                x.pk2,
                if (x.val1 == null) y.val1 else x.val1,
                Math.min(x.val2, y.val2),
                new Timestamp(Math.max(x.val3.getTime, y.val3.getTime))
              )
          }
      }
      .toSeq
  }

  /**
    * Reads the contents of a table into a sequence of [[TestData]].
    *
    * @param tableName table to read
    * @param connection connection to use
    * @return a sequence with the TestData read form table
    */
  def getTableContents(tableName: String, connection: Connection): Seq[TestData] = {
    val ss = connection.createStatement()
    val rs = ss.executeQuery(s"SELECT * FROM $tableName")

    var contents: Seq[TestData] = List.empty[TestData]
    while (rs.next()) {
      val row = TestData(
        pk1 = rs.getString("pk1"),
        pk2 = rs.getInt("pk2"),
        val1 = rs.getString("val1"),
        val2 = rs.getLong("val2"),
        val3 = rs.getTimestamp("val3")
      )
      contents = contents :+ row
    }

    rs.close()
    ss.close()

    contents
  }

  /**
    * Prints the contents of a table to stdout.
    *
    * @param tableName table to print
    * @param connection connection to use
    */
  def printTableContents(tableName: String, connection: Connection): Unit = {
    logger.info(s"Contents of table $tableName")
    val ss            = connection.createStatement()
    val rs            = ss.executeQuery(s"SELECT * FROM $tableName")
    val rsmd          = rs.getMetaData
    val columnsNumber = rsmd.getColumnCount

    logger.info((1 to columnsNumber).map(x => f"${rsmd.getColumnName(x)}%26s").mkString)
    while (rs.next()) {
      logger.info((1 to columnsNumber).map(x => f"${rs.getString(x)}%26s").mkString)
    }

    rs.close()
    ss.close()
  }

}
