package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.extension.ApiKeyCredentials
import akka.http.scaladsl.model.StatusCodes
import akka.http.scaladsl.model.headers.HttpChallenges
import akka.http.scaladsl.server.AuthenticationFailedRejection.{CredentialsMissing, CredentialsRejected}
import akka.http.scaladsl.server.Directives.{complete, get, pathEnd, pathPrefix, pathSingleSlash}
import akka.http.scaladsl.server.{AuthenticationFailedRejection, Route}
import akka.http.scaladsl.testkit.ScalatestRouteTest
import it.agilelab.bigdata.wasp.master.security.ApiKeyAuthenticationVerifier
import it.agilelab.bigdata.wasp.master.security.common.AuthenticationService
import it.agilelab.bigdata.wasp.utils.JsonSupport
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}


class SecurityApiKeyAuthenticationSpec
    extends FlatSpec
    with Matchers
    with ScalatestRouteTest
    with JsonSupport
    with BeforeAndAfterAll {

    var httpSecurity: AuthenticationService = _
    var secureRoute: Route = _
    var hashedKey: String = _

    override def beforeAll(): Unit = {
        super.beforeAll()

        // user api key value: TestCredentialsUser
        hashedKey = "14996B14D6892B4871C316188E9DA656B33CBCA35B9864E5E8C9EE3EFF6DF11C"

        httpSecurity = new ApiKeyAuthenticationVerifier {
            override protected val allowedKeys: Iterable[String] = Iterable(hashedKey)
        }

        secureRoute = pathPrefix("secure") {
            httpSecurity.authenticate { _ =>
                (pathEnd | pathSingleSlash) {
                    get {
                        complete("authentication successful")
                    }
                }
            }
        }
    }

    def addApiKey(credentials: ApiKeyCredentials): RequestTransformer = request =>
        request.withUri(s"${request.uri}?apiKey=${credentials.key}")

    "A service" should "reject if no credentials are passed" in {
        Get("/secure") ~> secureRoute ~> check {
            rejection shouldEqual AuthenticationFailedRejection(
                CredentialsMissing,
                HttpChallenges.basic("")
            )
        }
    }

    it should "reject if invalid credentials are passed" in {
        val invalidCredentials = ApiKeyCredentials("TestBadCredentials")
        Get("/secure") ~> addApiKey(invalidCredentials) ~> secureRoute ~> check {
            rejection shouldEqual AuthenticationFailedRejection(
                CredentialsRejected,
                HttpChallenges.basic("")
            )
        }
    }

    it should "invoke correctly if credentials are valid" in {
        val validCredentials = ApiKeyCredentials("TestCredentialsUser")
        Get("/secure") ~> addApiKey(validCredentials) ~> secureRoute ~> check {
            status shouldEqual StatusCodes.OK
            responseAs[String] shouldEqual "authentication successful"
        }
    }
}
