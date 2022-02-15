package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.{ContinuousUpdate, ParallelWrite, ParallelWriteModel}

object TestModels {
  val model1 = ParallelWriteModel(ParallelWrite("append"), entityDetails = CatalogCoordinates("default", "mock", "v1"))
  val model2 = ParallelWriteModel(ParallelWrite("overwrite"), entityDetails = CatalogCoordinates("default", "mock", "v1"))

  private val entityDetails: CatalogCoordinates = CatalogCoordinates("default", "mock", "v1")
  private val entityDetailsWithGlueDB: CatalogCoordinates = CatalogCoordinates("default", "mock", "v1", Some("mock_glue_db"))
  private val wrongDetails: CatalogCoordinates = CatalogCoordinates("default", "fake", "v1")
  private val notExistingEntityDetails: CatalogCoordinates = CatalogCoordinates("default", "entity", "v1")

  val modelWithGlue = ParallelWriteModel(ParallelWrite("overwrite"), entityDetails = entityDetailsWithGlueDB)

  val continuousUpdateModel1 = ParallelWriteModel(ContinuousUpdate(keys = "column1" :: Nil, orderingExpression = "ordering"), entityDetails)

  val continuousUpdateModel2 = ParallelWriteModel(ContinuousUpdate(keys = "column1" :: Nil, orderingExpression = "-(ordering1 + ordering2)"), entityDetails)

  val wrongModel = ParallelWriteModel(ContinuousUpdate(keys = "column1" :: Nil, orderingExpression = "-(ordering1 + ordering2)"), wrongDetails)

  val notExistingEntityModel = ParallelWriteModel(ContinuousUpdate(keys = "column1" :: Nil, orderingExpression = "-(ordering1 + ordering2)"), notExistingEntityDetails)
}
