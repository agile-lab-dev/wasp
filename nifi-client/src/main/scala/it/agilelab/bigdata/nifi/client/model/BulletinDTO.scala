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

case class BulletinDTO(
  /* The id of the bulletin. */
  id: Option[Long] = None,
  /* If clustered, the address of the node from which the bulletin originated. */
  nodeAddress: Option[String] = None,
  /* The category of this bulletin. */
  category: Option[String] = None,
  /* The group id of the source component. */
  groupId: Option[String] = None,
  /* The id of the source component. */
  sourceId: Option[String] = None,
  /* The name of the source component. */
  sourceName: Option[String] = None,
  /* The level of the bulletin. */
  level: Option[String] = None,
  /* The bulletin message. */
  message: Option[String] = None,
  /* When this bulletin was generated. */
  timestamp: Option[String] = None
) extends ApiModel


