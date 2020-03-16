package it.agilelab.bigdata.wasp.consumers.spark.batch

import com.typesafe.config.{Config, ConfigFactory}
import it.agilelab.bigdata.wasp.consumers.spark.MlModels.{MlModelsBroadcastDB, MlModelsDB}
import it.agilelab.bigdata.wasp.consumers.spark.strategies.Strategy
import it.agilelab.bigdata.wasp.consumers.spark.strategies.gdpr.GdprStrategy
import it.agilelab.bigdata.wasp.core.models._
import org.apache.spark.SparkContext

import scala.util.{Success, Try}

object BatchJobEtlExecution {

  def stepCreateStrategy(etl: BatchETL,
                         restConfig: Config,
                         sparkContext: SparkContext): Try[Option[Strategy]] = etl match {
    case batchETLModel: BatchETLModel => createStrategy(batchETLModel, restConfig, sparkContext)
    case batchGDPRModel: BatchGdprETLModel => createGdprStrategy(batchGDPRModel, restConfig, sparkContext)
  }

  def stepCreateMlModelsBroadcast(mlModelsDB: MlModelsDB,
                                  batchETL: BatchETL,
                                  sparkContext: SparkContext): Try[MlModelsBroadcastDB] = batchETL match {
    case batchETLModel: BatchETLModel => Try {
      mlModelsDB.createModelsBroadcast(batchETLModel.mlModels)(sparkContext)
    }
    case _: BatchGdprETLModel => Success(new MlModelsBroadcastDB())
  }

  private def createStrategy(batchETLModel: BatchETLModel,
                             restConfig: Config,
                             sparkContext: SparkContext): Try[Option[Strategy]] = batchETLModel.strategy match {
    case None => Success(None)
    case Some(strategyModel: StrategyModel) =>
      val result = Try {
        Class.forName(strategyModel.className)
          .newInstance()
          .asInstanceOf[Strategy]
      }
      val configuration = restConfig.withFallback(strategyModel.configurationConfig().getOrElse(ConfigFactory.empty()))
      result.map(_.configuration = configuration)
      result.map(_.sparkContext = Some(sparkContext))
      result.map(Some(_))
  }

  private def createGdprStrategy(batchETLModel: BatchGdprETLModel,
                                 restConfig: Config,
                                 sparkContext: SparkContext): Try[Some[GdprStrategy]] = {
    val result = Try {
      Class.forName(batchETLModel.strategy.className)
        .getConstructor(classOf[List[DataStoreConf]])
        .newInstance(batchETLModel.strategy.dataStoresConf)
        .asInstanceOf[GdprStrategy]
    }
    val configuration = restConfig.withFallback(batchETLModel.strategy.configurationConfig().getOrElse(ConfigFactory.empty()))
    result.map(_.configuration = configuration)
    result.map(_.sparkContext = Some(sparkContext))
    result.map(Some(_))
  }

}
