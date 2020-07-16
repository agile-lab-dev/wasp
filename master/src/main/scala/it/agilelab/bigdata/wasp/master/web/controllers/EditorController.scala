package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.AngularOkResponse
import it.agilelab.bigdata.wasp.models.editor.{ErrorDTO, PipegraphDTO}
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

class EditorController(editorService: EditorService, pipegraphService: PipegraphEditorService) extends Directives with JsonSupport {

  def getRoutes: Route = postEditor ~ putEditor ~ postPipegraph

  def postEditor: Route = extractExecutionContext { implicit ec =>
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

  def putEditor: Route = extractExecutionContext { implicit ec =>
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

  def postPipegraph: Route = extractExecutionContext { implicit ec =>
    pathPrefix("editor") {
      pathPrefix("pipegraph") {
        parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
          pathEnd {
            post {
              entity(as[PipegraphDTO]) { pipegraph =>
                complete {
                  pipegraph.toPipegraphModel match {
                    case Left(x) => x.toJson.toAngularKoResponse("KO", pretty)
                    case Right(x) =>
                      val errs: List[ErrorDTO] = pipegraphService.validatePipegraphModel(x)
                      if (errs.nonEmpty) errs.toJson.toAngularKoResponse("KO", pretty)
                      else "".toJson.toAngularOkResponse(pretty)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

}
