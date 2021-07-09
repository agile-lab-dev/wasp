package it.agilelab.bigdata.wasp.master.security.common

import akka.http.scaladsl.server.Directives.AsyncAuthenticatorPF

trait CredentialsVerifier {
    def verifyCredentials: AsyncAuthenticatorPF[Identity]
}
