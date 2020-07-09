package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.{File, IOException}
import java.util
import java.util.concurrent.{Executors, TimeUnit}

import it.agilelab.bigdata.wasp.core.logging.GuardedLogging
import it.agilelab.bigdata.wasp.models.configuration.HBaseConfigModel
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Admin, BufferedMutator, Connection, ConnectionFactory, Table}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.token.TokenIdentifier
import org.apache.hadoop.security.token.delegation.AbstractDelegationTokenIdentifier
import org.apache.hadoop.yarn.security.{AMRMTokenIdentifier, ContainerTokenIdentifier, NMTokenIdentifier}
import org.apache.hadoop.yarn.security.client.ClientToAMTokenIdentifier

import scala.collection.JavaConverters._


class HBaseConnection(hbaseConfig: Option[HBaseConfigModel]) extends GuardedLogging {

  // region members

  private val config: Configuration = {
    val conf = HBaseConfiguration.create()
    val document: Option[HBaseConfigModel] = hbaseConfig

    if (document.isDefined) {
      val hbaseConf = document.get
      val coreSiteXml = hbaseConf.coreSiteXmlPath
      val hbaseSiteXml = hbaseConf.hbaseSiteXmlPath
      val otherOpts = hbaseConf.others

      conf.addResource(new Path(coreSiteXml))
      conf.addResource(new Path(hbaseSiteXml))

      otherOpts.foreach {
        opt =>
          log.info(s"HBase additional custom option added: ${opt.key} -> ${opt.value}")
          conf.set(opt.key, opt.value)
      }

      log.info("HBaseConnection instance with explicit config")
    } else {
      log.warn("No hbase config retrieved. HBaseConnection will be initialized with default config")
    }
    log.info(printHBaseConf(conf))
    conf
  }

  private var _connection: Connection = _

  private lazy val env: util.Map[String, String] = System.getenv()

  private lazy val isWSenabled: Boolean = env.containsKey("WASP_SECURITY")

  //private lazy val choreService = new ChoreService("hbase_chores_")
  private lazy val executor = Executors.newSingleThreadScheduledExecutor()

  private val specialChars: String = "+++++-----"

  // endregion

  // region public methods

  def isConnectionNullOrClosed: Boolean = _connection == null || _connection.isClosed

  private def getConnection: Connection = {
    if (isConnectionNullOrClosed) {
      _connection = _initConn
    }
    _connection
  }

  def withTable[A](tableName: String)(f: Table => A): A = {
    try {
      val table = this.getConnection.getTable(TableName.valueOf(tableName))
      try {
        f(table)
      } finally {
        table.close()
      }
    } catch {
      case e: Exception =>
        closeConnection()
        log.error(s"$specialChars ERROR some error in the HBASE CONNECTION $specialChars", e)
        throw e
    }
  }

  def withMutator[A](tableName: String)(f: BufferedMutator => A): A = {
    try {
      val mutator = getConnection.getBufferedMutator(TableName.valueOf(tableName))
      try {
        f(mutator)
      } finally {
        mutator.flush()
        mutator.close()
      }
    } catch {
      case e: Exception =>
        closeConnection()
        log.error(s"$specialChars ERROR some error in the HBASE CONNECTION $specialChars", e)
        throw e
    }
  }

  def withAdmin[A](f: Admin => A): A = {
    try {
      val admin = getConnection.getAdmin
      try {
        f(admin)
      } finally {
        admin.close()
      }
    } catch {
      case e: Exception =>
        closeConnection()
        log.error(s"$specialChars ERROR some error in the HBASE CONNECTION $specialChars", e)
        throw e
    }
  }

  // endregion

  // region side effect methods

  def closeConnection(): Unit = synchronized {
    try {
      if (this._connection != null)
        this._connection.close()
    } finally {
      this._connection = null
    }
  }

  // endregion

  // region init methods

  private def _initConn = synchronized {
    try {
      log.info(s"Creating HBase connection. WASP_SECURITY is${if (!isWSenabled) "not " else " "}enabled")
      val result = if (isWSenabled) {
        val maybeToken = UserGroupInformation.getCurrentUser.getTokens.asScala
          .find(_.decodeIdentifier().isInstanceOf[AuthenticationTokenIdentifier])
        val user = maybeToken.map { token =>
          log.info(s"Found HBase token: ${tokenToString(token.decodeIdentifier())}")
          User.create(UserGroupInformation.getCurrentUser)
        }.getOrElse {
          val principal: String = System.getenv("PRINCIPAL_NAME")
          val keytabLocation: String = System.getenv("KEYTAB_FILE_NAME")
          log.info(s"Creating user in UGI with PRINCIPAL_NAME $principal, KEYTAB_FILE_NAME $keytabLocation")
          if (!keytabFileExists(keytabLocation)) {
            log.warn(s"Cannot check keytab file existence at $keytabLocation! Continuing...")
          }
          val confWithAuthKeys = newConfWithAuthKeys(config, principal, keytabLocation)

          UserGroupInformation.setConfiguration(confWithAuthKeys)
          val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabLocation)
          log.info(s"Logged user: $ugi")
          UserGroupInformation.setLoginUser(ugi)
          log.info(s"Setting UGI login user to $ugi")
          executor.scheduleAtFixedRate(
            new Runnable {
              override def run(): Unit = {
                try
                  ugi.checkTGTAndReloginFromKeytab()
                catch {
                  case e: IOException =>
                    log.error("Got exception while trying to refresh credentials: " + e.getMessage, e)
                }
              }
            }, 0, 30, TimeUnit.SECONDS
          )
          User.create(ugi)
        }
        ConnectionFactory.createConnection(config, user)
      } else {
        ConnectionFactory.createConnection(config)
      }
      executor.scheduleAtFixedRate(tokenLogRunnable, 0L, 10, TimeUnit.MINUTES)
      log.info(s"HBase connection created.")
      result
    }
    catch {
      case e: IOException =>
        log.error(s"Unable to connect to Hbase ${e.getMessage}")
        throw new RuntimeException(e)
    }
  }

  // endregion

  // region util methods

  private def newConfWithAuthKeys(conf: Configuration, principal: String, keytabLocation: String) = {
    val z: Configuration = new Configuration(conf)
    z.set("hbase.client.keytab.file", keytabLocation)
    z.set("hbase.client.kerberos.principal", principal)
    z
  }

  private def keytabFileExists(f: String): Boolean = {
    val file = new File(f)
    file.exists()
  }

  private def printHBaseConf(conf: Configuration) = {
    "HBase with properties:\n\t" +
      ("hbase.zookeeper.quorum" ::
        "hbase.zookeeper.property.clientPort" ::
        "hbase.master" ::
        "hadoop.security.authentication" ::
        "hbase.security.authentication" ::
        "hbase.cluster.distributed" ::
        "hbase.rpc.protection" ::
        "hbase.master.kerberos.principal" ::
        "hbase.regionserver.kerberos.principal" ::
        "hbase.client.retries.number" :: Nil).map(printConfKey(conf, _)).mkString("\n\t")
  }

  private lazy val tokenLogRunnable = {
    new Runnable {

      override def run(): Unit = {
        log.debug(currentTokensToString())
      }
    }
  }

  def currentTokensToString(): String = {
    "Current tokens:\n\t" +
      UserGroupInformation.getCurrentUser.getCredentials.getAllTokens.asScala
        .map(x => tokenToString(x.decodeIdentifier()))
        .mkString("\n\t")
  }

  def printConfKey(conf: Configuration, key: String): String = {
    s"$key: ${conf.get(key, "")}"
  }

  def tokenToString(token: TokenIdentifier): String = {
    token match {
      case o: AbstractDelegationTokenIdentifier =>
        o.toString
      case o: AuthenticationTokenIdentifier =>
        (new StringBuilder).append(o.getKind)
          .append(", keyId=").append(o.getKeyId)
          .append(", user=").append(o.getUser)
          .append(", issueDate=").append(o.getIssueDate)
          .append(", expirationDate=").append(o.getExpirationDate)
          .append(", sequenceNumber=").append(o.getSequenceNumber)
          .append(", username=").append(o.getUsername).toString()
      case o: AMRMTokenIdentifier =>
        (new StringBuilder).append(o.getKind)
          .append(", keyId=").append(o.getKeyId)
          .append(", user=").append(o.getUser).toString()
      case o: BlockTokenIdentifier =>
        o.toString
      case o: ContainerTokenIdentifier =>
        (new StringBuilder).append(o.getKind)
          .append(", keyId=").append(o.getMasterKeyId)
          .append(", user=").append(o.getUser)
          .append(", applicationSubmitter=").append(o.getApplicationSubmitter)
          .append(", expiryTimeStamp=").append(o.getExpiryTimeStamp)
          .append(", containerID=").append(o.getContainerID)
          .append(", creationTime=").append(o.getCreationTime).toString()
      case o: ClientToAMTokenIdentifier =>
        (new StringBuilder).append(o.getKind)
          .append(", user=").append(o.getUser)
          .append(", clientName=").append(o.getClientName).toString()
      case o: JobTokenIdentifier =>
        (new StringBuilder).append(o.getKind)
          .append(", jobId=").append(o.getJobId)
          .append(", user=").append(o.getUser).toString()
      case o: NMTokenIdentifier =>
        (new StringBuilder).append(o.getKind)
          .append(", keyId=").append(o.getKeyId)
          .append(", user=").append(o.getUser)
          .append(", applicationSubmitter=").append(o.getApplicationSubmitter).toString()
    }
  }

  // endregion

}


