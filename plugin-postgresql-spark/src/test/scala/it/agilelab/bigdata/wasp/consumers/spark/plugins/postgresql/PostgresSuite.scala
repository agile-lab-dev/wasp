package it.agilelab.bigdata.wasp.consumers.spark.plugins.postgresql

import org.scalatest.{BeforeAndAfterAll, Suite}
import io.zonky.test.db.postgres.embedded.EmbeddedPostgres

import java.util.UUID

trait PostgresSuite extends Suite with BeforeAndAfterAll {
  import PostgresSuite._

  lazy val jdbcUrl: String = pg.getJdbcUrl(user, db)
  lazy val connection      = pg.getPostgresDatabase.getConnection()

  override def beforeAll(): Unit = {
    super.beforeAll()
    connection
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if (!connection.isClosed) connection.close()
  }

}

object PostgresSuite {

  val db       = "postgres"
  val user     = "postgres"
  val password = "postgres"
  val driver   = "org.postgresql.Driver"

  private lazy val pg = {
    val uuid     = UUID.randomUUID()
    val basePath = s"/tmp/postgressuite-test-data/${uuid.toString}"
    System.setProperty("ot.epg.working-dir", basePath + "/workdir")
    val build = EmbeddedPostgres.builder()
    build.setCleanDataDirectory(true)
    build.setDataDirectory(basePath + "/data")
    build.setServerConfig("max_connections", "100")
    val _pg = build.start()
    sys.addShutdownHook(_pg.close())
    _pg
  }

}
