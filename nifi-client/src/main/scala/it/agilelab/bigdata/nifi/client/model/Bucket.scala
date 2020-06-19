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

case class Bucket(
  link: Option[JaxbLink] = None,
  /* An ID to uniquely identify this object. */
  identifier: Option[String] = None,
  /* The name of the bucket. */
  name: String,
  /* The timestamp of when the bucket was first created. This is set by the server at creation time. */
  createdTimestamp: Option[Long] = None,
  /* A description of the bucket. */
  description: Option[String] = None,
  /* Indicates if this bucket allows the same version of an extension bundle to be redeployed and thus overwrite the existing artifact. By default this is false. */
  allowBundleRedeploy: Option[Boolean] = None,
  /* Indicates if this bucket allows read access to unauthenticated anonymous users */
  allowPublicRead: Option[Boolean] = None,
  permissions: Option[Permissions] = None
) extends ApiModel

