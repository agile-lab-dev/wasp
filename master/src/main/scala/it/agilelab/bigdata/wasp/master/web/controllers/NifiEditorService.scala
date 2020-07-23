package it.agilelab.bigdata.wasp.master.web.controllers

import it.agilelab.bigdata.nifi.client.NifiClient
import it.agilelab.bigdata.wasp.models.editor.{NifiStatelessInstanceModel, ProcessGroupResponse}
import org.json4s.JsonAST.{JObject, JString, JValue}

import scala.concurrent.{ExecutionContext, Future}

trait NormalizeOps[A] {
  def normalize(error: String): Future[A]
}

trait NormalizeSupport {
  implicit def toNormalizeOps[A](option: Option[A]): NormalizeOps[A] = new NormalizeOps[A] {
    override def normalize(error: String): Future[A] = option match {
      case Some(a) => Future.successful(a)
      case None    => Future.failed(new Exception(error))
    }
  }
}

trait EditorService {

  def newEditorSession(processGroupName: String): Future[NifiStatelessInstanceModel]
  def commitEditorSession(processGroupId: String): Future[ProcessGroupResponse]

}

class NifiEditorService(nifiClient: NifiClient[Future])(implicit ec: ExecutionContext)
    extends EditorService
    with NormalizeSupport {

  override def newEditorSession(processGroupName: String): Future[NifiStatelessInstanceModel] = {
    for {
      rootId         <- nifiClient.flow.rootId
      processGroup   <- nifiClient.processGroups.create(rootId, processGroupName)
      processGroupId <- processGroup.id.normalize("Process group id not found")
      editorUrl      <- nifiClient.processGroups.editorUrl(processGroupId)
      _              <- nifiClient.processGroups.ports.input.create(processGroupId, "wasp-input", 700, 0)
      _              <- nifiClient.processGroups.ports.output.create(processGroupId, "wasp-output", 1000, 0)
      _              <- nifiClient.processGroups.ports.output.create(processGroupId, "wasp-error", positionX = 1000, positionY = 100)
    } yield (NifiStatelessInstanceModel(processGroupName, editorUrl, processGroupId))
  }

  def getErrorPort(flowContent: JObject): Option[String] = {
    (for {
      JObject(data)                       <- flowContent
      ("outputPorts", ports)              <- data
      JObject(port)                       <- ports
      ("name", JString(name))             <- port
      ("identifier", JString(identifier)) <- port if name == "wasp-error"
    } yield (identifier)).headOption
  }

  override def commitEditorSession(processGroupId: String): Future[ProcessGroupResponse] =
    for {
      processGroup <- nifiClient.processGroups.exportProcessGroup(processGroupId)
      (_, flowContent) <- processGroup
                           .findField(a => a._1 == "flowContents")
                           .normalize(
                             "flow contents not found"
                           )
      result: JObject <- coerceToJObjectOrError(flowContent)
      _               <- getErrorPort(result).normalize("No wasp-error port found")
    } yield (ProcessGroupResponse(processGroupId, result))

  private def coerceToJObjectOrError[T](flowContent: JValue): Future[JObject] = {
    if (flowContent.isInstanceOf[JObject]) {
      Future.successful(flowContent.asInstanceOf[JObject])
    } else {
      Future.failed(
        new Exception(s"Expected object as flowContent but got ${flowContent.getClass.getCanonicalName}")
      )
    }
  }

}
