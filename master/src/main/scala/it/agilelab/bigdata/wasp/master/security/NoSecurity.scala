package it.agilelab.bigdata.wasp.master.security

import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.Directives.{extractExecutionContext, provide}
import akka.http.scaladsl.server.directives.AuthenticationDirective
import it.agilelab.bigdata.wasp.master.security.common.{AuthenticationProvider, AuthenticationService, CredentialsVerifier, Identity}

import scala.concurrent.Future

/**
  * Does not perform authorization or authentication operations
  * meaning it will allow every user to access server
  */
class NoSecurity extends AuthenticationService with AuthenticationProvider with CredentialsVerifier {
    override def authentication: AuthenticationDirective[Identity] =
        extractExecutionContext.flatMap(_ => provide(Identity("")))

    override def verifyCredentials: Directives.AsyncAuthenticatorPF[Identity] = {
        case _ => Future.failed[Identity](new UnsupportedOperationException())
    }
}