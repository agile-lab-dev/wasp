package it.agilelab.bigdata.nifi.client

import java.util.UUID
import akka.stream.scaladsl._
import akka.util.ByteString
import it.agilelab.bigdata.nifi.client.api.{ControllerApi, FlowApi, ProcessGroupsApi, VersionsApi}
import it.agilelab.bigdata.nifi.client.core.SttpSerializer
import sttp.client.SttpBackend
import sttp.client.akkahttp.AkkaHttpBackend
import sttp.client.akkahttp.Types.LambdaFlow

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import it.agilelab.bigdata.nifi.client.core.ApiInvoker._
import it.agilelab.bigdata.nifi.client.core.alias.ApiRequestT
import it.agilelab.bigdata.nifi.client.model.{BucketsEntity, PortDTO, PortEntity, ProcessGroupDTO, ProcessGroupEntity, RegistryClientEntity, RevisionDTO, StartVersionControlRequestEntity, VersionedFlowDTO, VersionedFlowDTOEnums}
import sttp.client.monad.{FutureMonad, MonadError}


class NifiClient(nifiRawClient: NifiRawClient, clientId: UUID) {

  object id {
    def create[F[_]: MonadError]: F[String] = implicitly[MonadError[F]].unit(UUID.randomUUID().toString)
  }

  object flow {

    def rootId: ApiRequestT[String] = nifiRawClient.processGroups.getProcessGroup("root").mapResponseRight(_.id.get)

  }

  object registry {

    def registries: ApiRequestT[Set[RegistryClientEntity]] =
      nifiRawClient.controllers.getRegistryClients().mapResponseRight(_.registries.getOrElse(Set.empty))

    def buckets(registryId: String): ApiRequestT[BucketsEntity] =
      nifiRawClient.flows.getBuckets(registryId)

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
        def create(portId: String, portName: String): ApiRequestT[PortEntity] =
          nifiRawClient.processGroups.createInputPort(portId, newPort(portName))

      }

      object output {
        def create(parentId: String, portName: String): ApiRequestT[PortEntity] =
          nifiRawClient.processGroups.createOutputPort(parentId, newPort(portName))
      }

    }

    def create(parentId: String, name: String): ApiRequestT[ProcessGroupEntity] = {
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

      nifiRawClient.processGroups.createProcessGroup(parentId, entity)
    }
  }

}

class NifiRawClient(baseApi: String)(implicit sttpSerializer: SttpSerializer) {

  val processGroups: ProcessGroupsApi = ProcessGroupsApi(baseApi)

  val flows: FlowApi = FlowApi(baseApi)

  val controllers: ControllerApi = ControllerApi(baseApi)

  val versions: VersionsApi = VersionsApi(baseApi)

}

object NifiRawClient {
  def apply(baseApi: String)(implicit sttpSerializer: SttpSerializer): NifiRawClient =
    new NifiRawClient(baseApi)(sttpSerializer)
}

object Runner {

  def main(args: Array[String]): Unit = {

    implicit val akkaBackend: SttpBackend[Future, Source[ByteString, Any], LambdaFlow] =
      AkkaHttpBackend()
    implicit val serializer: SttpSerializer = new SttpSerializer()

    val rawClient = NifiRawClient("http://localhost:8080/nifi-api")

    implicit val ec             = ExecutionContext.global
    implicit val monadForFuture = new FutureMonad()(ExecutionContext.global)

    val nifi = new NifiClient(rawClient, UUID.randomUUID())

    val result = for {
      root       <- nifi.flow.rootId.result
      registries <- nifi.registry.registries.result
      buckets    <- nifi.registry.buckets(registries.head.id.get).result
      pg         <- nifi.processGroups.create(root, "demo").result
      input      <- nifi.processGroups.ports.input.create(pg.id.get, "best-input-ever").result
      output     <- nifi.processGroups.ports.output.create(pg.id.get, "best-output-ever").result
      startVersion <- rawClient.versions
                       .saveToFlowRegistry(
                         pg.id.get,
                         StartVersionControlRequestEntity(
                           processGroupRevision = pg.revision,
                           versionedFlow = Some(
                             VersionedFlowDTO(
                               registryId = registries.head.id,
                               bucketId = buckets.buckets.head.head.id,
                               action = Some(VersionedFlowDTOEnums.Action.COMMIT),
                               flowName = Some("demo")
                             )
                           )
                         )
                       )
                       .result

    } yield (startVersion)

    val res = Await.result(result, Duration.Inf)

    print(res)
    akkaBackend.close()

  }
}
