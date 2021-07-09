package it.agilelab.bigdata.wasp.master.security

/** Implementation of [[ApiKeyAuthenticationVerifier]] that verifies on a list of hashed keys
  */
class ApiKeyAuthenticationVerifierImpl(keyList: Iterable[String]) extends ApiKeyAuthenticationVerifier {
    override protected lazy val allowedKeys: Iterable[String] = keyList
}
