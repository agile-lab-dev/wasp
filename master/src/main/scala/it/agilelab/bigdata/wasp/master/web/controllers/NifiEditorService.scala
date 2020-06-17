package it.agilelab.bigdata.wasp.master.web.controllerseditorInstanceFormat

import it.agilelab.bigdata.nifi.client.NifiClient
import it.agilelab.bigdata.wasp.core.models.editor.NifiStatelessInstanceModel

import scala.concurrent.{ExecutionContext, Future}

class NifiEditorService(nifiClient: NifiClient[Future])(implicit ec: ExecutionContext) {

  def newEditorSession(processGroupName: String): Future[NifiStatelessInstanceModel] = {
    for {
      rootId       <- nifiClient.flow.rootId
      processGroup <- nifiClient.processGroups.create(rootId, processGroupName)
      editorUrl    <- nifiClient.processGroups.editorUrl(processGroup)
    } yield (NifiStatelessInstanceModel(processGroupName, editorUrl, processGroup.id.get))
  }

}
