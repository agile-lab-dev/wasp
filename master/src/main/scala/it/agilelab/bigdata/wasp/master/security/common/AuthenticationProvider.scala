package it.agilelab.bigdata.wasp.master.security.common

import akka.http.scaladsl.server.directives.AuthenticationDirective

trait AuthenticationProvider {
    self: CredentialsVerifier =>
    def authentication: AuthenticationDirective[Identity]
}
