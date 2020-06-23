package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.AngularOkResponse
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import spray.json._

class EditorController(editorService: EditorService) extends Directives with JsonSupport {

  def getRoutes: Route = postEditor ~ putEditor

  def postEditor = extractExecutionContext { implicit ec =>
    pathPrefix("editor") {
      pathPrefix("nifi") {
        pathPrefix(Segment) { processGroupName =>
          parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
            pathEnd {
              post {
                complete {
                  editorService.newEditorSession(processGroupName).map { editorInstance =>
                    new AngularOkResponse(editorInstance.toJson).toAngularOkResponse(pretty)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  def putEditor = extractExecutionContext { implicit ec =>
    pathPrefix("editor") {
      pathPrefix("nifi") {
        pathPrefix(Segment) { processGroupId =>
          parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
            pathEnd {
              put {
                complete {
                  editorService.commitEditorSession(processGroupId).map(_.toJson.toAngularOkResponse(pretty))
                }
              }
            }
          }
        }
      }
    }
  }

}
