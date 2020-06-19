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

case class VersionControlInformationDTO(
  /* The ID of the Process Group that is under version control */
  groupId: Option[String] = None,
  /* The ID of the registry that the flow is stored in */
  registryId: Option[String] = None,
  /* The name of the registry that the flow is stored in */
  registryName: Option[String] = None,
  /* The ID of the bucket that the flow is stored in */
  bucketId: Option[String] = None,
  /* The name of the bucket that the flow is stored in */
  bucketName: Option[String] = None,
  /* The ID of the flow */
  flowId: Option[String] = None,
  /* The name of the flow */
  flowName: Option[String] = None,
  /* The description of the flow */
  flowDescription: Option[String] = None,
  /* The version of the flow */
  version: Option[Int] = None,
  /* The current state of the Process Group, as it relates to the Versioned Flow */
  state: Option[VersionControlInformationDTOEnums.State] = None,
  /* Explanation of why the group is in the specified state */
  stateExplanation: Option[String] = None
) extends ApiModel

object VersionControlInformationDTOEnums {

  type State = State.Value
  object State extends Enumeration {
    val LOCALLYMODIFIED = Value("LOCALLY_MODIFIED")
    val STALE = Value("STALE")
    val LOCALLYMODIFIEDANDSTALE = Value("LOCALLY_MODIFIED_AND_STALE")
    val UPTODATE = Value("UP_TO_DATE")
    val SYNCFAILURE = Value("SYNC_FAILURE")
  }

}
