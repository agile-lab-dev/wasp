package it.agilelab.bigdata.wasp.consumers.spark.strategies

import it.agilelab.bigdata.wasp.consumers.spark.http.{Enricher, HttpEnricher}
import it.agilelab.bigdata.wasp.models.configuration.{RestEnrichmentConfigModel, RestEnrichmentSource}
import org.apache.spark.TaskContext

trait EnrichmentStrategy extends Strategy {

  private[consumers] var enricherConfig : RestEnrichmentConfigModel = null

  def enricher(sourceKey: String): Enricher = {

    val sourceInfo: RestEnrichmentSource = enricherConfig.sources.apply(sourceKey)

    val enricher = sourceInfo.kind.toLowerCase match {
      case "http" => new HttpEnricher(sourceInfo)
      case _ => Class.forName(sourceInfo.kind).newInstance().asInstanceOf[Enricher]
    }

    TaskContext.get().addTaskCompletionListener(task => {
      enricher.close()
    })

    enricher
  }
}
