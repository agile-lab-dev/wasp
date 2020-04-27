package it.agilelab.bigdata.wasp.master.web.controllers

import java.util.UUID

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.testkit.ScalatestRouteTest
import akka.stream.ActorMaterializer
import com.typesafe.config.{Config, ConfigFactory}
import it.agilelab.bigdata.wasp.core.models.IndexModelBuilder.Solr
import it.agilelab.bigdata.wasp.core.models.{BatchETLModel, BatchJobInstanceModel, BatchJobModel, IndexModel, IndexModelBuilder, JobStatus, LegacyStreamingETLModel, RawModel, ReaderModel, StrategyModel, WriterModel}
import it.agilelab.bigdata.wasp.master.web.utils.JsonSupport
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}
import spray.json.{DefaultJsonProtocol, JsObject, JsString, JsValue, JsonFormat, JsonParser, RootJsonFormat}

class MockBatchJobService extends BatchJobService {

  lazy val solr: IndexModel = IndexModelBuilder.forSolr
    .named("test_solr")
    .config(Solr.Config.default)
    .schema(
      Solr.Schema(
        Solr.Field("number", Solr.Type.TrieInt),
        Solr.Field("nested.field1", Solr.Type.String),
        Solr.Field("nested.field2", Solr.Type.TrieLong),
        Solr.Field("nested.field3", Solr.Type.String)
      )
    )
    .build
  lazy val flat = RawModel(
    name = "TestRawFlatSchemaModel",
    uri = "hdfs://" + System.getenv("HOSTNAME") + ":9000/user/root/test_flat/",
    timed = true,
    schema = StructType(
      Seq(
        StructField("id", StringType),
        StructField("number", LongType),
        StructField("nested.field1", StringType),
        StructField("nested.field2", LongType),
        StructField("nested.field3", StringType)
      )
    ).json
  )
  var storage = List(
    BatchJobModel(
      name = "TestBatchJobFromSolrToHdfs",
      description = "Description pf TestBatchJobFromSolr",
      owner = "user",
      system = false,
      creationTime = 0L,
      etl = BatchETLModel(
        name = "EtlModel for TestBatchJobFromSolr",
        inputs = List(ReaderModel.solrReader("Solr Reader", solr)),
        output = WriterModel.rawWriter("Raw Writer", flat),
        mlModels = List(),
        strategy = Some(
          StrategyModel.create(
            "it.agilelab.bigdata.wasp.whitelabel.consumers.spark.strategies.test.TestIdentityStrategy",
            ConfigFactory
              .parseString("""stringKey = "stringValue", intKey = 1""")
          )
        ),
        kafkaAccessType = LegacyStreamingETLModel.KAFKA_ACCESS_TYPE_DIRECT
      )
    )
  )
  var instances: List[BatchJobInstanceModel] = List.empty

  override def list(): Seq[BatchJobModel] = storage

  override def insert(batchJob: BatchJobModel): Unit = {
    storage = storage :+ batchJob
  }

  override def update(batchJob: BatchJobModel): Unit = {
    storage = storage.filterNot(m => m.name == batchJob.name) :+ batchJob
  }

  override def start(name: String,
                     restConfig: Config): Either[String, String] = {

    val maybeModel = storage.find(_.name == name)

    val maybeInstance = maybeModel.map { model =>
      BatchJobInstanceModel(
        name = s"${model.name}-${UUID.randomUUID().toString}",
        instanceOf = model.name,
        startTimestamp = System.currentTimeMillis(),
        currentStatusTimestamp = -1,
        status = JobStatus.PENDING,
        restConfig = restConfig
      )
    }

    if (maybeInstance.isDefined) {
      instances = instances :+ maybeInstance.get
      Right(
        JsObject(
          "startResult" -> JsString(s"Batch job '$name' start accepted'"),
          "instance" -> JsString(s"${maybeInstance.get.name}")
        ).toString
      )
    } else {
      val errorMessage = "Unknown error"
      Left(s"Batch job '$name' start not accepted due to [$errorMessage]")
    }

  }

  override def instance(instanceName: String): Option[BatchJobInstanceModel] =
    instances.find(_.name == instanceName)

  override def instanceOf(name: String): Seq[BatchJobInstanceModel] =
    instances.filter(_.instanceOf == name)

  override def delete(batchJob: BatchJobModel): Unit = {
    storage = storage.filterNot(_.name == batchJob.name)
  }

  override def get(name: String): Option[BatchJobModel] =
    storage.find(_.name == name)
}

case class AngularResponse[T](Result: String, data: T)
case class BatchJobStartResult(startResult: String, instance: String)

class BatchJobControllerSpec
    extends FlatSpec
    with ScalatestRouteTest
    with Matchers
    with JsonSupport {

  implicit def angularResponse[T: JsonFormat]
    : RootJsonFormat[AngularResponse[T]] = jsonFormat2(AngularResponse.apply[T])
  implicit val batchJobStartResult: RootJsonFormat[BatchJobStartResult] =
    jsonFormat2(BatchJobStartResult.apply)

  it should "Respond to get requests" in {
    val service = new MockBatchJobService()
    val controller = new BatchJobController(service)

    Get("/batchjobs") ~> controller.getRoute ~> check {
      val response = responseAs[AngularResponse[List[BatchJobModel]]]
      response shouldEqual AngularResponse("OK", service.storage)
    }

  }

  it should "insert new batch jobs" in {
    val service = new MockBatchJobService()
    val controller = new BatchJobController(service)

    val batchJob = service.storage.head.copy(name = "TestBatch")

    Post("/batchjobs", batchJob) ~> controller.getRoute ~> check {
      service.storage.length shouldBe 2
      service.storage(1).name shouldBe "TestBatch"
      responseAs[AngularResponse[String]] shouldEqual AngularResponse("OK", "OK")
    }
  }

  it should "update other batch jobs" in {
    val service = new MockBatchJobService()
    val controller = new BatchJobController(service)

    val batchJob = service.storage.head.copy(owner = "ciccio")

    Put("/batchjobs", batchJob) ~> controller.getRoute ~> check {
      service.storage.head shouldBe batchJob
      service.storage.length shouldBe 1
      responseAs[AngularResponse[String]] shouldEqual AngularResponse("OK", "OK")
    }
  }

  it should "It should delete batch jobs" in {
    val service = new MockBatchJobService()
    val controller = new BatchJobController(service)

    Delete("/batchjobs/TestBatchJobFromSolrToHdfs") ~> controller.getRoute ~> check {
      val response = responseAs[AngularResponse[String]]
      service.storage should be(empty)
      response shouldEqual AngularResponse("OK", "OK")
    }
  }

  it should "It should start jobs" in {
    val service = new MockBatchJobService()
    val controller = new BatchJobController(service)

    Post("/batchjobs/TestBatchJobFromSolrToHdfs/start") ~> controller.getRoute ~> check {
      val response = responseAs[AngularResponse[BatchJobStartResult]]

      response shouldEqual AngularResponse(
        "OK",
        BatchJobStartResult(
          "Batch job 'TestBatchJobFromSolrToHdfs' start accepted'",
          service.instances.head.name
        )
      )
    }
  }

  it should "It should start jobs with rest config" in {
    val service = new MockBatchJobService()
    val controller = new BatchJobController(service)

    val restConfig =  ConfigFactory.parseString("""
        |{
        | "ciccio": "pasticcio"
        |}
        |""".stripMargin)


    Post("/batchjobs/TestBatchJobFromSolrToHdfs/start", restConfig) ~> controller.getRoute ~>  check {
      val response = responseAs[AngularResponse[BatchJobStartResult]]

      service.instances.head.restConfig shouldEqual(restConfig)

      response shouldEqual AngularResponse(
        "OK",
        BatchJobStartResult(
          "Batch job 'TestBatchJobFromSolrToHdfs' start accepted'",
          service.instances.head.name
        )
      )
    }
  }

  it should "list instances" in {
    val service = new MockBatchJobService()
    val controller = new BatchJobController(service)

    service.start("TestBatchJobFromSolrToHdfs", ConfigFactory.empty())

    Get("/batchjobs/TestBatchJobFromSolrToHdfs/instances") ~> controller.getRoute ~> check {
      val response = responseAs[AngularResponse[List[BatchJobInstanceModel]]]

      response shouldEqual AngularResponse(
        "OK",
        service.instances
      )
    }
  }


  it should "retrieve specific instances" in {
    val service = new MockBatchJobService()
    val controller = new BatchJobController(service)

    service.start("TestBatchJobFromSolrToHdfs", ConfigFactory.empty())
    val expectedInstance = service.instances.head

    Get(s"/batchjobs/TestBatchJobFromSolrToHdfs/instances/${expectedInstance.name}") ~> controller.getRoute ~> check {
      val response = responseAs[AngularResponse[BatchJobInstanceModel]]

      response shouldEqual AngularResponse(
        "OK",
        expectedInstance
      )
    }
  }
}
