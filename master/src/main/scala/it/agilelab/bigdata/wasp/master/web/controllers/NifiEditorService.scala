package it.agilelab.bigdata.wasp.master.web.controllers

import it.agilelab.bigdata.nifi.client.NifiClient
import it.agilelab.bigdata.wasp.core.models.editor.NifiStatelessInstanceModel

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

}
