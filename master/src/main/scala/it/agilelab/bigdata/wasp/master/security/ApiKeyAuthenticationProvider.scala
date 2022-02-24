package it.agilelab.bigdata.wasp.master.security

import akka.http.extension.{ApiKeyCredentials, SecurityUtils}
import akka.http.scaladsl.model.headers.{HttpChallenges, HttpCredentials}
import akka.http.scaladsl.server.{AuthenticationFailedRejection, Directive1}
import akka.http.scaladsl.server.Directives.{AsyncAuthenticator, AuthenticationResult}
import akka.http.scaladsl.server.directives.{AuthenticationDirective, AuthenticationResult, Credentials}
import akka.http.scaladsl.server.directives.BasicDirectives.{extractExecutionContext, provide}
import akka.http.scaladsl.server.AuthenticationFailedRejection.{CredentialsMissing, CredentialsRejected}
import akka.http.scaladsl.server.directives.FutureDirectives.onSuccess
import akka.http.scaladsl.server.directives.ParameterDirectives._
import akka.http.scaladsl.server.directives.RouteDirectives.reject
import akka.http.scaladsl.util.FastFuture
import akka.http.scaladsl.util.FastFuture._
import it.agilelab.bigdata.wasp.master.security.common.{AuthenticationProvider, CredentialsVerifier, Identity}

import scala.concurrent.Future

/** API key implementation of [[AuthenticationProvider]]
  * Extracts credential value from query parameters of request
  */
trait ApiKeyAuthenticationProvider extends AuthenticationProvider with SecurityUtils {
    self: CredentialsVerifier =>

    val API_KEY: String = "apiKey"

    override def authentication: AuthenticationDirective[Identity] =
        extractExecutionContext.flatMap { implicit ec =>
            authenticateApiKeyAsync(
                credentials =>
                    if (verifyCredentials isDefinedAt credentials) verifyCredentials(credentials).fast.map(Some(_))
                    else FastFuture.successful(None))
        }

    private def authenticateApiKeyAsync(
                                           authenticator: AsyncAuthenticator[Identity]
                                       ): AuthenticationDirective[Identity] =
        extractExecutionContext.flatMap { implicit ec =>
            extractCredentialsAndAuthenticateOrRejectWithChallenge[ApiKeyCredentials, Identity](
                apiKeyCredentials,
                cred =>
                    authenticator(cred match {
                        case Some(ApiKeyCredentials(key)) =>
                            new Credentials.Provided(key) {
                                def verify(secret: String, hasher: String => String): Boolean = secret secure_== hasher(key)
                            }
                        case _ => Credentials.Missing
                    }).fast.map {
                        case Some(t) => AuthenticationResult.success(t)
                        case None    => AuthenticationResult.failWithChallenge(HttpChallenges.basic(""))
                    }
            )
        }

    private def extractCredentialsAndAuthenticateOrRejectWithChallenge[C <: HttpCredentials, T](
                                                                                                   credentialsExtractor: Directive1[Option[C]],
                                                                                                   authenticator: Option[C] => Future[AuthenticationResult[T]]
                                                                                               ): AuthenticationDirective[T] =
        credentialsExtractor.flatMap { cred =>
            onSuccess(authenticator(cred)).flatMap {
                case Right(user) => provide(user)
                case Left(challenge) =>
                    val cause = if (cred.isEmpty) CredentialsMissing else CredentialsRejected
                    reject(AuthenticationFailedRejection(cause, challenge)): Directive1[T]
            }
        }

    private def apiKeyCredentials: Directive1[Option[ApiKeyCredentials]] =
        parameterMap.map(m => m.get(API_KEY).map(x => ApiKeyCredentials(x)))
}
