package it.agilelab.bigdata.wasp.master.security

import akka.http.extension.SecurityUtils
import akka.http.scaladsl.server.Directives.AsyncAuthenticatorPF
import akka.http.scaladsl.server.directives.Credentials
import it.agilelab.bigdata.wasp.master.security.common.{AuthenticationService, CredentialsVerifier, Identity}

import scala.concurrent.Future

/** Verify if user can authenticate using provided API keys
  */
trait ApiKeyAuthenticationVerifier
    extends ApiKeyAuthenticationProvider
        with AuthenticationService
        with CredentialsVerifier
        with SecurityUtils {

    protected val allowedKeys: Iterable[String]

    private def isAllowedKey(credentials: Credentials.Provided): Boolean =
        allowedKeys.exists(key => credentials.verify(key, hash))

    override def verifyCredentials: AsyncAuthenticatorPF[Identity] = {
        case credentials @ Credentials.Provided(apiKey) if isAllowedKey(credentials) =>
            Future.successful(Identity(apiKey))
    }
}
