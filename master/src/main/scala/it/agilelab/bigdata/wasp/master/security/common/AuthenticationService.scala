package it.agilelab.bigdata.wasp.master.security.common

import akka.http.scaladsl.server.directives.AuthenticationDirective

trait AuthenticationService {
    self: AuthenticationProvider with CredentialsVerifier =>
    def authenticate: AuthenticationDirective[Identity] = authentication
}
