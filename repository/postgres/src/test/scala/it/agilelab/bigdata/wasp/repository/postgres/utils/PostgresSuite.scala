package it.agilelab.bigdata.wasp.repository.postgres.utils

import java.sql.Connection
import com.opentable.db.postgres.embedded.EmbeddedPostgres
import PostgresSuite._
import it.agilelab.bigdata.wasp.core.logging.Logging
import it.agilelab.bigdata.wasp.models.configuration.PostgresDBConfigModel
import it.agilelab.bigdata.wasp.repository.postgres.WaspPostgresDBImpl
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers, Outcome}


object PostgresSuite {
  private lazy val pg = {
    val time = System.currentTimeMillis()
    System.setProperty("ot.epg.working-dir", s"target/test-pg/$time")
    val build = EmbeddedPostgres.builder()
    build.setCleanDataDirectory(true)
    build.setDataDirectory(s"target/test-pg-data/$time")
    build.setServerConfig("max_connections", "200")
    val _pg = build.start()
    sys.addShutdownHook(_pg.close())
    _pg
  }


}

trait PostgresSuite extends FlatSpec with Matchers with BeforeAndAfterAll with Logging {

  private def getConnection: Connection = {
    val _conn = pg.getPostgresDatabase.getConnection()
    _conn.setAutoCommit(false)
    _conn
  }

  private def areWeOnAppleSilicon() = {
    System.getProperty("os.name") == "Mac OS X" && System.getProperty("os.arch") == "aarch64"
  }

  protected lazy val connection = getConnection


  override protected def withFixture(test: NoArgTest): Outcome = {
    this.assume(!areWeOnAppleSilicon(),"Postgres suite will not run on Apple Silicon")
    test()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    if(!areWeOnAppleSilicon()) {
      if (!connection.isClosed) connection.close()
      closePool()
    }
  }


  val user = "postgres"
  val pass = "postgres"
  lazy val jdbcUrl: String = pg.getJdbcUrl(user, "postgres")
  val driver = "org.postgresql.Driver"


  def closePool():Unit = ConnectionSupport.poolingDriver.closePool(s"$jdbcUrl:$user")

  lazy val config: PostgresDBConfigModel = PostgresDBConfigModel(jdbcUrl,user,pass,driver,10)
  lazy val pgDB = new WaspPostgresDBImpl(config)

}

