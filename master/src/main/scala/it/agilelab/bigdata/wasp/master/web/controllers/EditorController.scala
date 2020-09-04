package it.agilelab.bigdata.wasp.master.web.controllers

import akka.http.scaladsl.server.{Directives, Route}
import it.agilelab.bigdata.wasp.master.web.utils.JsonResultsHelper.AngularOkResponse
import it.agilelab.bigdata.wasp.models.editor.{ErrorDTO, PipegraphDTO}
import it.agilelab.bigdata.wasp.utils.JsonSupport
import spray.json._

class EditorController(editorService: EditorService, pipegraphService: PipegraphEditorService)
    extends Directives
    with JsonSupport {

  def getRoutes: Route =
    postEditor ~ putEditor ~ getEditorPipegraphs ~ getEditorPipegraph ~ postEditorPipegraph ~ putEditorPipegraph

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

  /**
    * Gets all pipegraphs created by editor
    * @return Array of pipegraphs created by editor
    */
  def getEditorPipegraphs: Route = extractExecutionContext { implicit ec =>
    pathPrefix("editor") {
      pathPrefix("pipegraph") {
        parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
          pathEnd {
            get {
              complete {
                pipegraphService.getAllUIPipegraphs.map(pipegraphService.toDTO).toJson.toAngularOkResponse(pretty)
              }
            }

          }
        }
      }
    }
  }

  /**
    * Gets specific pipegraph created by editor
    * @return Pipegraph created by editor
    */
  def getEditorPipegraph: Route = extractExecutionContext { implicit ec =>
    pathPrefix("editor") {
      pathPrefix("pipegraph") {
        pathPrefix(Segment) { pipegraphName =>
          parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
            pathEnd {
              get {
                complete {
                  pipegraphService.getUIPipegraph(pipegraphName).map(pipegraphService.toDTO) match {
                    case Some(x) => x.toJson.toAngularOkResponse(pretty)
                    case None =>
                      List(ErrorDTO.notFound("UI Pipegraph", pipegraphName)).toJson.toAngularKoResponse("KO", pretty)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  /**
    * Validates and inserts new pipegraphs done in FE editor
    * @return Empty OK response or KO response with array of errors (as strings)
    */
  def postEditorPipegraph: Route = extractExecutionContext { implicit ec =>
    pathPrefix("editor") {
      pathPrefix("pipegraph") {
        parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
          pathEnd {
            post {
              entity(as[PipegraphDTO]) { pipegraph =>
                complete {
                  pipegraphService.toPipegraphModel(pipegraph) match {
                    case Left(x) => x.toJson.toAngularKoResponse("KO", pretty)
                    case Right(x) =>
                      pipegraphService.insertPipegraphModel(x)
                      "".toJson.toAngularOkResponse(pretty)
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  /**
    * Validates and updates existing pipegraphs done in FE editor
    * @return Empty OK response or KO response with array of errors (as strings)
    */
  def putEditorPipegraph: Route = extractExecutionContext { implicit ec =>
    pathPrefix("editor") {
      pathPrefix("pipegraph") {
        parameters('pretty.as[Boolean].?(false)) { (pretty: Boolean) =>
          pathEnd {
            put {
              entity(as[PipegraphDTO]) { pipegraph =>
                complete {
                  pipegraphService.toPipegraphModel(pipegraph, isUpdate = true) match {
                    case Left(x) => x.toJson.toAngularKoResponse("KO", pretty)
                    case Right(x) =>
                      pipegraphService.updatePipegraphModel(x)
                      "".toJson.toAngularOkResponse(pretty)
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
