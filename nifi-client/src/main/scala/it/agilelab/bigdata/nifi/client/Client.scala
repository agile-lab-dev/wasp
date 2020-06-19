package it.agilelab.bigdata.nifi.client

import java.util.UUID

import it.agilelab.bigdata.nifi.client.api.{ControllerApi, FlowApi, ProcessGroupsApi, VersionsApi}
import it.agilelab.bigdata.nifi.client.core.ApiInvoker._
import it.agilelab.bigdata.nifi.client.core.SttpSerializer
import it.agilelab.bigdata.nifi.client.model._

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
      private def newPort(portName: String) = {
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
              )
            )
          )
        )
      }

      object input {
        def create(portId: String, portName: String): F[PortEntity] =
          nifiRawClient.processGroups.createInputPort(portId, newPort(portName)).result
      }

      object output {
        def create(parentId: String, portName: String): F[PortEntity] =
          nifiRawClient.processGroups.createOutputPort(parentId, newPort(portName)).result
      }

    }

    def editorUrl(processGroupEntity: ProcessGroupEntity): F[String] =
      implicitly[MonadError[F]].unit(s"${uiUrl}/?processGroupId=${processGroupEntity.id.getOrElse("")}")

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
