package it.agilelab.bigdata.wasp.repository.core.dbModels

import it.agilelab.bigdata.wasp.models.PipegraphStatus.PipegraphStatus
import it.agilelab.bigdata.wasp.models.{DashboardModel, LegacyStreamingETLModel, Model, RTModel, StructuredStreamingETLModel}
import it.agilelab.bigdata.wasp.models.configuration.RestEnrichmentConfigModel

trait PipegraphDBModel extends Model
trait PipegraphInstanceDBModel extends Model

case class PipegraphDBModelV1(override val name: String,
                            description: String,
                            owner: String,
                            isSystem: Boolean,
                            creationTime: Long,
                            legacyStreamingComponents: List[LegacyStreamingETLModel],
                            structuredStreamingComponents: List[StructuredStreamingETLModel],
                            rtComponents: List[RTModel],
                            dashboard: Option[DashboardModel] = None,
                            labels: Set[String] = Set.empty,
                            enrichmentSources: RestEnrichmentConfigModel = RestEnrichmentConfigModel(Map.empty)
                           ) extends PipegraphDBModel

case class PipegraphInstanceDBModelV1(
                                    override val name:String,
                                    instanceOf: String,
                                    startTimestamp: Long,
                                    currentStatusTimestamp: Long,
                                    status: PipegraphStatus,
                                    executedByNode: Option[String],
                                    peerActor: Option[String],
                                    error: Option[String] = None
                                     ) extends PipegraphInstanceDBModel