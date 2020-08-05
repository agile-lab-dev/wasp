package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.server.{Directives, Route}
import akka.stream.scaladsl.{Flow, Sink, Source}

import scala.concurrent.Future

class NifiProxyController(basePath: String, target: Uri) extends Directives {

  def defaultIfNotExist(headers: List[HttpHeader], name: String, defaultValue: String): List[HttpHeader] = {
    if(!headers.exists(header => header.is(name.toLowerCase))) {
      List[HttpHeader](RawHeader(name, defaultValue))
    } else {
      List[HttpHeader]()
    }
  }

  def getRoutes: Route = extractActorSystem { system =>
    pathPrefix(basePath) {
      path("nifi") {
        val uri: Uri = "/" + basePath + "/nifi/"
        redirect(uri, StatusCodes.Found)
      } ~
        extractRequest { request =>
          extractMaterializer { implicit materializer =>
            extractUnmatchedPath { rest: Uri.Path =>
              extractExecutionContext { implicit ec =>
                Route { context =>
                  val host = request.uri.authority.host.toString()
                  val port = request.uri.authority.port.toString

                  val headerList = request.headers.toList
                  val newHeaders = headerList ++
                    defaultIfNotExist(headerList, "X-ProxyScheme", request.uri.scheme) ++
                    defaultIfNotExist(headerList, "X-ProxyPort", port) ++
                    defaultIfNotExist(headerList, "X-ProxyHost", host) ++
                    defaultIfNotExist(headerList, "X-ProxyContextPath", basePath)

                  val proxyRequest = request.copy(
                    uri = request.uri.copy(
                      scheme = target.scheme,
                      authority = target.authority,
                      path = rest
                    ),
                    headers = newHeaders
                  )
                  val flow: Flow[HttpRequest, HttpResponse, Future[Http.OutgoingConnection]] =
                    Http(system).outgoingConnection(target.authority.host.address(), target.authority.port)

                  Source
                    .single(proxyRequest)
                    .via(flow)
                    .map(response =>
                      response.copy(headers = response.headers
                        .filterNot(h => h.name() == "X-Frame-Options" || h.name() == "Content-Security-Policy")
                      )
                    )
                    .runWith(Sink.head)
                    .flatMap(context.complete(_))

                }
              }
            }
          }
        }
    }
  }
}
