package it.agilelab.bigdata.wasp.whitelabel.models.test

import it.agilelab.bigdata.wasp.models.{HttpModel, HttpCompression}

object TestHttpModel {

  lazy val hostname = Option(System.getenv("HOSTNAME")).getOrElse("localhost")
  lazy val httpPost = HttpModel(
    name = "test_http_post",
    url = s"http://${hostname}:4480/",
    method = "POST",
    headersFieldName = None,
    valueFieldsNames = List.empty,
    compression = HttpCompression.Disabled,
    mediaType = "text/plain",
    logBody = false
  )
  lazy val httpPostHeaders = HttpModel(
    name = "test_http_post_headers",
    url = s"http://${hostname}:4480/",
    method = "POST",
    headersFieldName = Some("headers"),
    valueFieldsNames = List.empty,
    compression = HttpCompression.Disabled,
    mediaType = "text/plain",
    logBody = false
  )
  lazy val httpsPost = HttpModel(
    name = "test_https_post",
    url = s"http://${hostname}:4480/",
    method = "POST",
    headersFieldName = None,
    valueFieldsNames = List.empty,
    compression = HttpCompression.Disabled,
    mediaType = "text/plain",
    logBody = false
  )


}
