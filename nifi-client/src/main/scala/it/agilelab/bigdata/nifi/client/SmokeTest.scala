package it.agilelab.bigdata.nifi.client

import java.util.UUID

import akka.stream.scaladsl._
import akka.util.ByteString
import it.agilelab.bigdata.nifi.client.core.ApiInvoker._
import it.agilelab.bigdata.nifi.client.core.SttpSerializer
import it.agilelab.bigdata.nifi.client.model.{StartVersionControlRequestEntity, VersionedFlowDTO, VersionedFlowDTOEnums}
import sttp.client.SttpBackend
import sttp.client.akkahttp.AkkaHttpBackend
import sttp.client.akkahttp.Types.LambdaFlow
import sttp.client.monad.FutureMonad

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}


object SmokeTest {

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
