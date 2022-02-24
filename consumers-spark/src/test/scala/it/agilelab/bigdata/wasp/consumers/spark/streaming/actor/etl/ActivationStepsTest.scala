package it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl

import it.agilelab.bigdata.wasp.DatastoreModelsForTesting
import it.agilelab.bigdata.wasp.consumers.spark.eventengine.SparkSetup
import it.agilelab.bigdata.wasp.consumers.spark.strategies.{EnrichmentStrategy, EventIndexingStrategy, FreeCodeStrategy, ReaderKey, Strategy}
import it.agilelab.bigdata.wasp.consumers.spark.streaming.actor.etl.ActivationSteps.{StaticReaderFactory, StreamingReaderFactory}
import it.agilelab.bigdata.wasp.models.configuration.{RestEnrichmentConfigModel, RestEnrichmentSource}
import it.agilelab.bigdata.wasp.repository.core.bl.{FreeCodeBL, MlModelBL, TopicBL}
import it.agilelab.bigdata.wasp.repository.core.bl.{FreeCodeBL, MlModelBL, ProcessGroupBL, TopicBL}
import it.agilelab.bigdata.wasp.models.{FreeCodeModel, PipegraphModel, StrategyModel, StreamingReaderModel, StructuredStreamingETLModel, WriterModel}
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.tools.reflect.ToolBoxError
import scala.util.Try

class ActivationStepsTest extends FlatSpec with Matchers with SparkSetup {

  val defaultPipegraph = PipegraphModel(
    name = "pipegraph",
    description = "",
    owner = "test",
    isSystem = false,
    creationTime = System.currentTimeMillis(),
    structuredStreamingComponents = List(
      StructuredStreamingETLModel(
        name = "component",
        streamingInput = StreamingReaderModel.kafkaReader("", DatastoreModelsForTesting.TopicModels.json, None),
        staticInputs = List.empty,
        streamingOutput = WriterModel.solrWriter("", DatastoreModelsForTesting.IndexModels.solr),
        mlModels = List(),
        strategy = None,
        triggerIntervalMs = None,
        options = Map()
      )
    ),
    dashboard = None
  )

  "this" should "not create a strategy" in withSparkSession { ss =>
    val aSM         = new ActivationStepsMock(ss)
    val etl         = StructuredStreamingETLModel("name", "dafault", null, List.empty, null, List.empty, None, None)
    val strategyTry = aSM.createStrategy(etl, defaultPipegraph)
    strategyTry.isSuccess shouldBe true
    strategyTry.get.isEmpty shouldBe true
  }

  "this" should "throw a exception" in withSparkSession { ss =>
    val aSM           = new ActivationStepsMock(ss)
    val strategyModel = StrategyModel("it.agilelab.bigdata.wasp.consumers.spark.strategies.WRONG")
    val etl =
      StructuredStreamingETLModel("name", "dafault", null, List.empty, null, List.empty, Some(strategyModel), None)
    val strategyTry = aSM.createStrategy(etl, defaultPipegraph)
    strategyTry.isFailure shouldBe true
    strategyTry.failed.get shouldBe a[ClassNotFoundException]
  }

  "this" should "create a EventIndexingStrategy" in withSparkSession { ss =>
    val aSM           = new ActivationStepsMock(ss)
    val strategyModel = StrategyModel("it.agilelab.bigdata.wasp.consumers.spark.strategies.EventIndexingStrategy")
    val etl =
      StructuredStreamingETLModel("name", "dafault", null, List.empty, null, List.empty, Some(strategyModel), None)
    val strategyTry = aSM.createStrategy(etl, defaultPipegraph)
    strategyTry.isSuccess shouldBe true
    strategyTry.get.isEmpty shouldBe false
    strategyTry.get.get shouldBe a[EventIndexingStrategy]
  }

  "this" should "create a FreeCodeStrategy without config" in withSparkSession { ss =>
    val aSM           = new ActivationStepsMock(ss)
    val strategyModel = StrategyModel("it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeStrategy")
    val etl =
      StructuredStreamingETLModel("name", "dafault", null, List.empty, null, List.empty, Some(strategyModel), None)
    val strategyTry = aSM.createStrategy(etl, defaultPipegraph)
    strategyTry.isFailure shouldBe true
    strategyTry.failed.get shouldBe a[IllegalArgumentException]
  }

  "this" should "create a FreeCodeStrategy with config wrong" in withSparkSession { ss =>
    val aSM = new ActivationStepsMock(ss)
    val strategyModel =
      StrategyModel("it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeStrategy", Some(s"""{XXX:"test"}"""))
    val etl =
      StructuredStreamingETLModel("name", "dafault", null, List.empty, null, List.empty, Some(strategyModel), None)
    val strategyTry = aSM.createStrategy(etl, defaultPipegraph)
    strategyTry.isFailure shouldBe true
    strategyTry.failed.get shouldBe a[IllegalArgumentException]
  }

  "this" should "create a FreeCodeStrategy without FreeCodeBL" in withSparkSession { ss =>
    val aSM = new ActivationStepsMock(ss, null)
    val strategyModel =
      StrategyModel("it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeStrategy", Some(s"""{name:"test"}"""))
    val etl =
      StructuredStreamingETLModel("name", "dafault", null, List.empty, null, List.empty, Some(strategyModel), None)
    val strategyTry = aSM.createStrategy(etl, defaultPipegraph)
    strategyTry.isFailure shouldBe true
    strategyTry.failed.get shouldBe a[NullPointerException]
  }

  "this" should "create a FreeCodeStrategy without freeCodeName on db" in withSparkSession { ss =>
    val aSM = new ActivationStepsMock(ss)
    val strategyModel =
      StrategyModel("it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeStrategy", Some(s"""{name:"test"}"""))
    val etl =
      StructuredStreamingETLModel("name", "dafault", null, List.empty, null, List.empty, Some(strategyModel), None)
    val strategyTry = aSM.createStrategy(etl, defaultPipegraph)
    strategyTry.isFailure shouldBe true
    strategyTry.failed.get shouldBe a[IllegalArgumentException]
  }

  val enrichmentPipegraph =
    PipegraphModel(
      name = "pipegraph",
      description = "",
      owner = "test",
      isSystem = false,
      creationTime = System.currentTimeMillis(),
      structuredStreamingComponents = List(
        StructuredStreamingETLModel(
          name = "component",
          streamingInput = StreamingReaderModel.kafkaReader("", DatastoreModelsForTesting.TopicModels.json, None),
          staticInputs = List.empty,
          streamingOutput = WriterModel.solrWriter("", DatastoreModelsForTesting.IndexModels.solr),
          mlModels = List(),
          strategy = None,
          triggerIntervalMs = None,
          options = Map()
        )
      ),
      dashboard = None,
      enrichmentSources =
        RestEnrichmentConfigModel(
          Map.apply("httpExample" ->
            RestEnrichmentSource("http",
              Map.apply(
                "method" -> "get",
                "url" -> s"http://localhost:8080/$${author}-v1/{generic}/v2/$${api_test}/123?author=pippo"
              )
            ),
            "msExample" ->
              RestEnrichmentSource("it.agilelab.bigdata.wasp.consumers.spark.http.CustomEnricher",
                Map.apply(
                  "msName" -> "SampleMs"
                )
              )
          )
        )
    )

  "this" should "create a correct CustomEnrichmentStrategy" in withSparkSession { ss =>
    val aSM         = new ActivationStepsMock(ss)
    val strategyModel =
      StrategyModel("it.agilelab.bigdata.wasp.consumers.spark.http.etl.CustomEnrichmentStrategy", None)
    val etl =
      StructuredStreamingETLModel("name", "dafault", null, List.empty, null, List.empty, Some(strategyModel), None)

    val strategyTry = aSM.createStrategy(etl, enrichmentPipegraph)
    val config = strategyTry.get.get.asInstanceOf[EnrichmentStrategy].enricherConfig
    config shouldBe enrichmentPipegraph.enrichmentSources
  }

  "this" should "create a FreeCodeStrategy with freeCode wrong" in withSparkSession { ss =>
    val aSM           = new ActivationStepsMock(ss)
    val freeCodeModel = FreeCodeModel("test", "dataFramesWrong")
    val strategyModel = StrategyModel(
      "it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeStrategy",
      Some(s"""{name:"${freeCodeModel.name}"}""")
    )

    aSM.freeCodeBL.insert(freeCodeModel)
    val etl =
      StructuredStreamingETLModel("name", "dafault", null, List.empty, null, List.empty, Some(strategyModel), None)
    val strategyTry = aSM.createStrategy(etl, defaultPipegraph)
    strategyTry.isFailure shouldBe true
    strategyTry.failed.get shouldBe a[ToolBoxError]
  }

  "this" should "create a correct FreeCodeStrategy with columns wrong" in withSparkSession { ss =>
    val aSM = new ActivationStepsMock(ss)
    val freeCodeModel = FreeCodeModel(
      "test",
      """dataFrames.getFirstDataFrame.withColumn("column_test",concat(col("column_test"),lit("pippo"))) """
    )
    val strategyModel = StrategyModel(
      "it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeStrategy",
      Some(s"""{name:"${freeCodeModel.name}"}""")
    )

    aSM.freeCodeBL.insert(freeCodeModel)
    val etl =
      StructuredStreamingETLModel("name", "dafault", null, List.empty, null, List.empty, Some(strategyModel), None)
    val strategyTry = aSM.createStrategy(etl, defaultPipegraph)
    strategyTry.isSuccess shouldBe true
    strategyTry.get.get shouldBe a[FreeCodeStrategy]
    val df  = ss.createDataFrame(List((1, "1"), (2, "2"), (3, "3")))
    val map = Map(ReaderKey("test", "test") -> df)
    an[Exception] should be thrownBy strategyTry.get.get.transform(map).count
  }

  "this" should "create a correct FreeCodeStrategy" in withSparkSession { ss =>
    val aSM = new ActivationStepsMock(ss)
    val freeCodeModel = FreeCodeModel(
      "test",
      """dataFrames.getFirstDataFrame.withColumn("column_test",concat(col("_2"),lit("pippo"))) """
    )
    val strategyModel = StrategyModel(
      "it.agilelab.bigdata.wasp.consumers.spark.strategies.FreeCodeStrategy",
      Some(s"""{name:"${freeCodeModel.name}"}""")
    )

    aSM.freeCodeBL.insert(freeCodeModel)
    val etl =
      StructuredStreamingETLModel("name", "dafault", null, List.empty, null, List.empty, Some(strategyModel), None)
    val strategyTry = aSM.createStrategy(etl, defaultPipegraph)
    strategyTry.isSuccess shouldBe true
    strategyTry.get.get shouldBe a[FreeCodeStrategy]
    val df     = ss.createDataFrame(List((1, "1"), (2, "2"), (3, "3")))
    val map    = Map(ReaderKey("test", "test") -> df)
    val output = strategyTry.get.get.transform(map).cache()
    output.count() shouldBe 3
    output.rdd.collect().toSeq.map(r => r.getAs[Int]("_1")) should contain theSameElementsAs List(1, 2, 3)
    output.rdd.collect().toSeq.map(r => r.getAs[String]("_2")) should contain theSameElementsAs List("1", "2", "3")
    output.rdd.collect().toSeq.map(r => r.getAs[String]("column_test")) should contain theSameElementsAs List(
      "1pippo",
      "2pippo",
      "3pippo"
    )
  }

}

class ActivationStepsMock(
    override protected val sparkSession: SparkSession,
    override val freeCodeBL: FreeCodeBL = new FreeCodeBLMock
) extends ActivationSteps {

  override protected val mlModelBl: MlModelBL           = null
  override protected val topicsBl: TopicBL              = null
  override protected val processGroupBL: ProcessGroupBL = null

  override protected val streamingReaderFactory: StreamingReaderFactory = null
  override protected val staticReaderFactory: StaticReaderFactory       = null

  override def createStrategy(etl: StructuredStreamingETLModel, pipegraph: PipegraphModel): Try[Option[Strategy]] = {
    super.createStrategy(etl, pipegraph)

  }
}

class FreeCodeBLMock extends FreeCodeBL {

  private val list = ListBuffer.empty[FreeCodeModel]

  override def getByName(name: String): Option[FreeCodeModel] = list.find(_.name.equals(name))

  override def deleteByName(name: String): Unit = {
    val index = list.zipWithIndex.filter(_._1.name.equals(name)).map(_._2)
    index.foreach(list.drop)
  }

  override def getAll: Seq[FreeCodeModel] = list

  override def insert(freeCodeModel: FreeCodeModel): Unit = {
    list += freeCodeModel
  }

  override def upsert(freeCodeModel: FreeCodeModel): Unit = {
    list += freeCodeModel
  }
}
