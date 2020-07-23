package it.agilelab.bigdata.nifi.client

import java.util.UUID

import it.agilelab.bigdata.nifi.client.api._
import it.agilelab.bigdata.nifi.client.core.ApiInvoker._
import it.agilelab.bigdata.nifi.client.core.SttpSerializer
import it.agilelab.bigdata.nifi.client.model.{VersionControlInformationEntity, _}
import org.json4s.JObject
import sttp.client.SttpBackend
import sttp.client.monad.MonadError

class NifiClient[F[_]: MonadError](nifiRawClient: NifiRawClient, clientId: UUID, uiUrl: String)(
    implicit sttpBackend: SttpBackend[F, Nothing, Nothing]
) {

  object id {

    def create: F[String] = implicitly[MonadError[F]].unit(UUID.randomUUID().toString)

  }

  object flow {

    def rootId: F[String] = nifiRawClient.processGroups.getProcessGroup("root").mapResponseRight(_.id.get).result

  }

  object registry {

    def registries: F[Set[RegistryClientEntity]] =
      nifiRawClient.controllers.getRegistryClients().mapResponseRight(_.registries.getOrElse(Set.empty)).result

    def buckets(registryId: String): F[BucketsEntity] =
      nifiRawClient.flows.getBuckets(registryId).result
  }

  object processGroups {

    object ports {
      private def newPort(portName: String, positionX: Double, positionY: Double) = {
        PortEntity(
          revision = Some(
            RevisionDTO(
              clientId = Some(
                clientId.toString
              ),
              version = Some(
                0
              )
            )
          ),
          component = Some(
            PortDTO(
              name = Some(
                portName
              ),
              position = Some(
                PositionDTO(
                  x = Some(positionX),
                  y = Some(positionY)
                )
              )
            )
          )
        )
      }

      object input {
        def create(
            processGroupId: String,
            portName: String,
            positionX: Double = 0,
            positionY: Double = 0
        ): F[PortEntity] =
          nifiRawClient.processGroups.createInputPort(processGroupId, newPort(portName, positionX, positionY)).result
      }

      object output {
        def create(
            processGroupId: String,
            portName: String,
            positionX: Double = 0,
            positionY: Double = 0
        ): F[PortEntity] =
          nifiRawClient.processGroups.createOutputPort(processGroupId, newPort(portName, positionX, positionY)).result
      }

    }

    def editorUrl(processGroupId: String): F[String] =
      implicitly[MonadError[F]].unit(s"${uiUrl}/?processGroupId=${processGroupId}")

    def create(parentId: String, name: String): F[ProcessGroupEntity] = {
      val entity = ProcessGroupEntity(
        revision = Some(
          RevisionDTO(
            clientId = Some(clientId.toString),
            version = Some(0)
          )
        ),
        component = Some(
          ProcessGroupDTO(
            parentGroupId = Some(parentId),
            name = Some(name)
          )
        )
      )

      nifiRawClient.processGroups.createProcessGroup(parentId, entity).result
    }

    def getProcessGroup(processGroupId: String) =
      nifiRawClient.processGroups.getProcessGroup(processGroupId).result

    def exportProcessGroup(processGroupId: String): F[JObject] =
      nifiRawClient.processGroups.exportProcessGroup(processGroupId).result

  }

  object versions {
    def saveToFlowRegistry(
        processGroupId: String,
        entity: StartVersionControlRequestEntity
    ): F[VersionControlInformationEntity] =
      nifiRawClient.versions.saveToFlowRegistry(processGroupId, entity).result

    def getVersionInformation(processGroupId: String): F[VersionControlInformationEntity] =
      nifiRawClient.versions.getVersionInformation(processGroupId).result

    def updateFlowVersion(processGroupId: String, versionedFlowSnapshotEntity: VersionedFlowSnapshotEntity): F[VersionControlInformationEntity] =
      nifiRawClient.versions.updateFlowVersion(processGroupId, versionedFlowSnapshotEntity).result

    def initiateVersionControlUpdate(
        processGroupId: String,
        versionControlInformationEntity: VersionControlInformationEntity
    ): F[VersionedFlowUpdateRequestEntity] =
      nifiRawClient.versions.initiateVersionControlUpdate(processGroupId, versionControlInformationEntity).result

    def getVersionedProcessGroup(id: String): F[JObject] = {
      nifiRawClient.versions.exportFlowVersion(id).result
    }
  }

}

class NifiRawClient(apiUrl: String)(implicit sttpSerializer: SttpSerializer) {

  val processGroups: ProcessGroupsApi = ProcessGroupsApi(apiUrl)

  val flows: FlowApi = FlowApi(apiUrl)

  val controllers: ControllerApi = ControllerApi(apiUrl)

  val versions: VersionsApi = VersionsApi(apiUrl)

}
object NifiRawClient {
  def apply(apiUrl: String)(implicit sttpSerializer: SttpSerializer): NifiRawClient =
    new NifiRawClient(apiUrl)(sttpSerializer)
}
