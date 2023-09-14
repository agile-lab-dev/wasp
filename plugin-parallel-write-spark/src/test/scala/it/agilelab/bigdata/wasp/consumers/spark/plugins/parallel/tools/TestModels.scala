package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools

import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.catalog.CatalogCoordinates
import it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.model.{ContinuousUpdate, ParallelWrite, ParallelWriteModel}

object TestModels {
  val model1 = ParallelWriteModel(ParallelWrite("append"), entityDetails = CatalogCoordinates("default", "mock", "v1"))
  val model2 = ParallelWriteModel(ParallelWrite("overwrite"), entityDetails = CatalogCoordinates("default", "mock", "v1"))

  private val entityDetails: CatalogCoordinates = CatalogCoordinates("default", "mock", "v1")
  private val entityDetailsWithDB: CatalogCoordinates = CatalogCoordinates("default", "mock", "v1", Some("mock_db"))
  private val wrongDetails: CatalogCoordinates = CatalogCoordinates("default", "fake", "v1")
  private val notExistingEntityDetails: CatalogCoordinates = CatalogCoordinates("default", "entity", "v1")

  val modelWithDB = ParallelWriteModel(ParallelWrite("overwrite"), entityDetails = entityDetailsWithDB)

  val continuousUpdateModel1 = ParallelWriteModel(ContinuousUpdate(keys = "column1" :: Nil, orderingExpression = "ordering", Some(100), Some(64), Some(168), Some(100)), entityDetails)

  val continuousUpdateModel2 = ParallelWriteModel(ContinuousUpdate(keys = "column1" :: Nil, orderingExpression = "-(ordering1 + ordering2)", Some(100), Some(64), Some(168), Some(100)), entityDetails)

  val wrongModel = ParallelWriteModel(ContinuousUpdate(keys = "column1" :: Nil, orderingExpression = "-(ordering1 + ordering2)", Some(100), Some(64), Some(168), Some(100)), wrongDetails)

  val notExistingEntityModel = ParallelWriteModel(ContinuousUpdate(keys = "column1" :: Nil, orderingExpression = "-(ordering1 + ordering2)", Some(100), Some(64), Some(168), Some(100)), notExistingEntityDetails)
}
