package it.agilelab.bigdata.wasp.consumers.spark.plugins.plain.hbase.integration

import org.apache.hadoop.hbase.security.token.AuthenticationTokenIdentifier
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.internal.Logging

import java.util.Date
import scala.collection.JavaConverters._

object HBaseCredentialsManager extends Logging with Serializable {

  def applyCredentials[T](): Unit = {
    val close = Option(System.getProperty("it.agilelab.bigdata.wasp.hbase.connection.close")).exists(_.toBoolean)

    if (close) {
      HBaseConnectionCache.performHousekeeping(true)
    }

    def stringifyToken(tokenIdentifier: AuthenticationTokenIdentifier): String = {
      Seq(
        ("Username", tokenIdentifier.getUsername),
        ("SequenceNumber", tokenIdentifier.getSequenceNumber),
        ("KeyId", tokenIdentifier.getKeyId),
        ("IssueDate", new Date(tokenIdentifier.getIssueDate)),
        ("ExpirationDate", new Date(tokenIdentifier.getExpirationDate))
      ).map {
        case (name, value) => s"$name=$value"
      }.mkString(", ")
    }

    val tokenInfo = UserGroupInformation.getCurrentUser
      .getCredentials
      .getAllTokens
      .asScala
      .map(_.decodeIdentifier())
      .filter(_.isInstanceOf[AuthenticationTokenIdentifier])
      .map(_.asInstanceOf[AuthenticationTokenIdentifier])
      .map(stringifyToken)
      .headOption.getOrElse("NoTokenFound")


    logDebug(s"Tokens -> $tokenInfo")
  }

}
