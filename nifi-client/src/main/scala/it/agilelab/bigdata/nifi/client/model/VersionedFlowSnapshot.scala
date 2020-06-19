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
package it.agilelab.bigdata.nifi.client.model

import it.agilelab.bigdata.nifi.client.core.ApiModel

case class VersionedFlowSnapshot(
  snapshotMetadata: VersionedFlowSnapshotMetadata,
  flowContents: VersionedProcessGroup,
  /* The information about controller services that exist outside this versioned flow, but are referenced by components within the versioned flow. */
  externalControllerServices: Option[Map[String, ExternalControllerServiceReference]] = None,
  /* The parameter contexts referenced by process groups in the flow contents. The mapping is from the name of the context to the context instance, and it is expected that any context in this map is referenced by at least one process group in this flow. */
  parameterContexts: Option[Map[String, VersionedParameterContext]] = None,
  /* The optional encoding version of the flow contents. */
  flowEncodingVersion: Option[String] = None,
  flow: Option[VersionedFlow] = None,
  bucket: Option[Bucket] = None,
  latest: Option[Boolean] = None
) extends ApiModel

