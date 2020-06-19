package it.agilelab.bigdata.wasp.master.web.controllers

import it.agilelab.bigdata.nifi.client.NifiClient
import it.agilelab.bigdata.wasp.core.models.editor.NifiStatelessInstanceModel

import scala.concurrent.{ExecutionContext, Future}


trait EditorService {

  def newEditorSession(processGroupName: String) : Future[NifiStatelessInstanceModel]

}

class NifiEditorService(nifiClient: NifiClient[Future])(implicit ec: ExecutionContext) extends EditorService {

  override def newEditorSession(processGroupName: String): Future[NifiStatelessInstanceModel] = {
    for {
      rootId       <- nifiClient.flow.rootId
      processGroup <- nifiClient.processGroups.create(rootId, processGroupName)
      editorUrl    <- nifiClient.processGroups.editorUrl(processGroup)
    } yield (NifiStatelessInstanceModel(processGroupName, editorUrl, processGroup.id.get))
  }

}
