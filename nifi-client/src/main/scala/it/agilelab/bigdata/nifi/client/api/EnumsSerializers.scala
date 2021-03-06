/**
 * NiFi Rest Api
 * The Rest Api provides programmatic access to command and control a NiFi instance in real time. Start and                                              stop processors, monitor queues, query provenance data, and more. Each endpoint below includes a description,                                             definitions of the expected input and output, potential response codes, and the authorizations required                                             to invoke each service.
 *
 * The version of the OpenAPI document: 1.11.4
 * Contact: dev@nifi.apache.org
 *
 * NOTE: This class is auto generated by OpenAPI Generator (https://openapi-generator.tech).
 * https://openapi-generator.tech
 * Do not edit the class manually.
 */
package it.agilelab.bigdata.nifi.client.api

import it.agilelab.bigdata.nifi.client.model.{AccessPolicyDTOEnums, AccessPolicySummaryDTOEnums, ActivateControllerServicesEntityEnums, AffectedComponentDTOEnums, AffectedComponentEntityEnums, ComponentValidationResultDTOEnums, ConnectableComponentEnums, ConnectableDTOEnums, ConnectionDTOEnums, ConnectionEntityEnums, ControllerServiceDTOEnums, ControllerServiceReferencingComponentDTOEnums, ControllerServiceRunStatusEntityEnums, ControllerServiceStatusDTOEnums, FlowBreadcrumbEntityEnums, LineageRequestDTOEnums, PortDTOEnums, PortRunStatusEntityEnums, PortStatusDTOEnums, PortStatusSnapshotDTOEnums, ProcessGroupEntityEnums, ProcessGroupStatusSnapshotDTOEnums, ProcessorDTOEnums, ProcessorRunStatusEntityEnums, ProcessorStatusDTOEnums, ProcessorStatusSnapshotDTOEnums, ProvenanceNodeDTOEnums, RemotePortRunStatusEntityEnums, RemoteProcessGroupStatusDTOEnums, ReportingTaskDTOEnums, ReportingTaskRunStatusEntityEnums, ReportingTaskStatusDTOEnums, ScheduleComponentsEntityEnums, UpdateControllerServiceReferenceRequestEntityEnums, VersionControlInformationDTOEnums, VersionedConnectionEnums, VersionedControllerServiceEnums, VersionedFlowDTOEnums, VersionedFlowEnums, VersionedFunnelEnums, VersionedLabelEnums, VersionedPortEnums, VersionedProcessGroupEnums, VersionedProcessorEnums, VersionedRemoteGroupPortEnums, VersionedRemoteProcessGroupEnums}
import org.json4s._

import scala.reflect.ClassTag

object EnumsSerializers {

  def all: Seq[Serializer[_]] = Seq[Serializer[_]]() :+
    new EnumNameSerializer(AccessPolicyDTOEnums.Action) :+
    new EnumNameSerializer(AccessPolicySummaryDTOEnums.Action) :+
    new EnumNameSerializer(ActivateControllerServicesEntityEnums.State) :+
    new EnumNameSerializer(AffectedComponentDTOEnums.ReferenceType) :+
    new EnumNameSerializer(AffectedComponentEntityEnums.ReferenceType) :+
    new EnumNameSerializer(ComponentValidationResultDTOEnums.ReferenceType) :+
    new EnumNameSerializer(ConnectableComponentEnums.`Type`) :+
    new EnumNameSerializer(ConnectableDTOEnums.`Type`) :+
    new EnumNameSerializer(ConnectionDTOEnums.LoadBalanceStrategy) :+
    new EnumNameSerializer(ConnectionDTOEnums.LoadBalanceCompression) :+
    new EnumNameSerializer(ConnectionDTOEnums.LoadBalanceStatus) :+
    new EnumNameSerializer(ConnectionEntityEnums.SourceType) :+
    new EnumNameSerializer(ConnectionEntityEnums.DestinationType) :+
    new EnumNameSerializer(ControllerServiceDTOEnums.State) :+
    new EnumNameSerializer(ControllerServiceDTOEnums.ValidationStatus) :+
    new EnumNameSerializer(ControllerServiceReferencingComponentDTOEnums.ReferenceType) :+
    new EnumNameSerializer(ControllerServiceRunStatusEntityEnums.State) :+
    new EnumNameSerializer(ControllerServiceStatusDTOEnums.RunStatus) :+
    new EnumNameSerializer(ControllerServiceStatusDTOEnums.ValidationStatus) :+
    new EnumNameSerializer(FlowBreadcrumbEntityEnums.VersionedFlowState) :+
    new EnumNameSerializer(LineageRequestDTOEnums.LineageRequestType) :+
    new EnumNameSerializer(PortDTOEnums.State) :+
    new EnumNameSerializer(PortDTOEnums.`Type`) :+
    new EnumNameSerializer(PortRunStatusEntityEnums.State) :+
    new EnumNameSerializer(PortStatusDTOEnums.RunStatus) :+
    new EnumNameSerializer(PortStatusSnapshotDTOEnums.RunStatus) :+
    new EnumNameSerializer(ProcessGroupEntityEnums.VersionedFlowState) :+
    new EnumNameSerializer(ProcessGroupStatusSnapshotDTOEnums.VersionedFlowState) :+
    new EnumNameSerializer(ProcessorDTOEnums.State) :+
    new EnumNameSerializer(ProcessorDTOEnums.ValidationStatus) :+
    new EnumNameSerializer(ProcessorRunStatusEntityEnums.State) :+
    new EnumNameSerializer(ProcessorStatusDTOEnums.RunStatus) :+
    new EnumNameSerializer(ProcessorStatusSnapshotDTOEnums.RunStatus) :+
    new EnumNameSerializer(ProcessorStatusSnapshotDTOEnums.ExecutionNode) :+
    new EnumNameSerializer(ProvenanceNodeDTOEnums.`Type`) :+
    new EnumNameSerializer(RemotePortRunStatusEntityEnums.State) :+
    new EnumNameSerializer(RemoteProcessGroupStatusDTOEnums.ValidationStatus) :+
    new EnumNameSerializer(ReportingTaskDTOEnums.State) :+
    new EnumNameSerializer(ReportingTaskDTOEnums.ValidationStatus) :+
    new EnumNameSerializer(ReportingTaskRunStatusEntityEnums.State) :+
    new EnumNameSerializer(ReportingTaskStatusDTOEnums.RunStatus) :+
    new EnumNameSerializer(ReportingTaskStatusDTOEnums.ValidationStatus) :+
    new EnumNameSerializer(ScheduleComponentsEntityEnums.State) :+
    new EnumNameSerializer(UpdateControllerServiceReferenceRequestEntityEnums.State) :+
    new EnumNameSerializer(VersionControlInformationDTOEnums.State) :+
    new EnumNameSerializer(VersionedConnectionEnums.LoadBalanceStrategy) :+
    new EnumNameSerializer(VersionedConnectionEnums.LoadBalanceCompression) :+
    new EnumNameSerializer(VersionedConnectionEnums.ComponentType) :+
    new EnumNameSerializer(VersionedControllerServiceEnums.ComponentType) :+
    new EnumNameSerializer(VersionedFlowEnums.`Type`) :+
    new EnumNameSerializer(VersionedFlowDTOEnums.Action) :+
    new EnumNameSerializer(VersionedFunnelEnums.ComponentType) :+
    new EnumNameSerializer(VersionedLabelEnums.ComponentType) :+
    new EnumNameSerializer(VersionedPortEnums.`Type`) :+
    new EnumNameSerializer(VersionedPortEnums.ScheduledState) :+
    new EnumNameSerializer(VersionedPortEnums.ComponentType) :+
    new EnumNameSerializer(VersionedProcessGroupEnums.ComponentType) :+
    new EnumNameSerializer(VersionedProcessorEnums.ScheduledState) :+
    new EnumNameSerializer(VersionedProcessorEnums.ComponentType) :+
    new EnumNameSerializer(VersionedRemoteGroupPortEnums.ComponentType) :+
    new EnumNameSerializer(VersionedRemoteGroupPortEnums.ScheduledState) :+
    new EnumNameSerializer(VersionedRemoteProcessGroupEnums.TransportProtocol) :+
    new EnumNameSerializer(VersionedRemoteProcessGroupEnums.ComponentType)

  private class EnumNameSerializer[E <: Enumeration: ClassTag](enum: E)
    extends Serializer[E#Value] {
    import JsonDSL._

    val EnumerationClass: Class[E#Value] = classOf[E#Value]

    def deserialize(implicit format: Formats):
    PartialFunction[(TypeInfo, JValue), E#Value] = {
      case (t @ TypeInfo(EnumerationClass, _), json) if isValid(json) =>
        json match {
          case JString(value) =>
            enum.withName(value)
          case value =>
            throw new MappingException(s"Can't convert $value to $EnumerationClass")
        }
    }

    private[this] def isValid(json: JValue) = json match {
      case JString(value) if enum.values.exists(_.toString == value) => true
      case _ => false
    }

    def serialize(implicit format: Formats): PartialFunction[Any, JValue] = {
      case i: E#Value => i.toString
    }
  }

}
