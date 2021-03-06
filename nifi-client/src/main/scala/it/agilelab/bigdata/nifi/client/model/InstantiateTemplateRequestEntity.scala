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

case class InstantiateTemplateRequestEntity(
  /* The x coordinate of the origin of the bounding box where the new components will be placed. */
  originX: Option[Double] = None,
  /* The y coordinate of the origin of the bounding box where the new components will be placed. */
  originY: Option[Double] = None,
  /* The identifier of the template. */
  templateId: Option[String] = None,
  /* The encoding version of the flow snippet. If not specified, this is automatically populated by the node receiving the user request. If the snippet is specified, the version will be the latest. If the snippet is not specified, the version will come from the underlying template. These details need to be replicated throughout the cluster to ensure consistency. */
  encodingVersion: Option[String] = None,
  snippet: Option[FlowSnippetDTO] = None,
  /* Acknowledges that this node is disconnected to allow for mutable requests to proceed. */
  disconnectedNodeAcknowledged: Option[Boolean] = None
) extends ApiModel


