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

import it.agilelab.bigdata.nifi.client.core.SttpSerializer
import it.agilelab.bigdata.nifi.client.core.alias._
import it.agilelab.bigdata.nifi.client.model.{LineageEntity, ProvenanceEntity, ProvenanceOptionsEntity}
import sttp.client._
import sttp.model.Method

object ProvenanceApi {

  def apply(baseUrl: String = "http://localhost/nifi-api")(implicit serializer: SttpSerializer) = new ProvenanceApi(baseUrl)
}

class ProvenanceApi(baseUrl: String)(implicit serializer: SttpSerializer) {

  
  import serializer._

  /**
   * Expected answers:
   *   code 200 : LineageEntity (successful operation)
   *   code 400 :  (NiFi was unable to complete the request because it was invalid. The request should not be retried without modification.)
   *   code 401 :  (Client could not be authenticated.)
   *   code 403 :  (Client is not authorized to make this request.)
   *   code 404 :  (The specified resource could not be found.)
   *   code 409 :  (The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.)
   * 
   * @param id The id of the lineage query.
   * @param clusterNodeId The id of the node where this query exists if clustered.
   */
  def deleteLineage(id: String, clusterNodeId: Option[String] = None): ApiRequestT[LineageEntity] =
    basicRequest
      .method(Method.DELETE, uri"$baseUrl/provenance/lineage/${id}?clusterNodeId=$clusterNodeId")
      .contentType("application/json")
      .response(asJson[LineageEntity])

  /**
   * Expected answers:
   *   code 200 : ProvenanceEntity (successful operation)
   *   code 400 :  (NiFi was unable to complete the request because it was invalid. The request should not be retried without modification.)
   *   code 401 :  (Client could not be authenticated.)
   *   code 403 :  (Client is not authorized to make this request.)
   *   code 404 :  (The specified resource could not be found.)
   *   code 409 :  (The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.)
   * 
   * @param id The id of the provenance query.
   * @param clusterNodeId The id of the node where this query exists if clustered.
   */
  def deleteProvenance(id: String, clusterNodeId: Option[String] = None): ApiRequestT[ProvenanceEntity] =
    basicRequest
      .method(Method.DELETE, uri"$baseUrl/provenance/${id}?clusterNodeId=$clusterNodeId")
      .contentType("application/json")
      .response(asJson[ProvenanceEntity])

  /**
   * Expected answers:
   *   code 200 : LineageEntity (successful operation)
   *   code 400 :  (NiFi was unable to complete the request because it was invalid. The request should not be retried without modification.)
   *   code 401 :  (Client could not be authenticated.)
   *   code 403 :  (Client is not authorized to make this request.)
   *   code 404 :  (The specified resource could not be found.)
   *   code 409 :  (The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.)
   * 
   * @param id The id of the lineage query.
   * @param clusterNodeId The id of the node where this query exists if clustered.
   */
  def getLineage(id: String, clusterNodeId: Option[String] = None): ApiRequestT[LineageEntity] =
    basicRequest
      .method(Method.GET, uri"$baseUrl/provenance/lineage/${id}?clusterNodeId=$clusterNodeId")
      .contentType("application/json")
      .response(asJson[LineageEntity])

  /**
   * Expected answers:
   *   code 200 : ProvenanceEntity (successful operation)
   *   code 400 :  (NiFi was unable to complete the request because it was invalid. The request should not be retried without modification.)
   *   code 401 :  (Client could not be authenticated.)
   *   code 403 :  (Client is not authorized to make this request.)
   *   code 404 :  (The specified resource could not be found.)
   *   code 409 :  (The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.)
   * 
   * @param id The id of the provenance query.
   * @param clusterNodeId The id of the node where this query exists if clustered.
   * @param summarize Whether or not incremental results are returned. If false, provenance events are only returned once the query completes. This property is true by default.
   * @param incrementalResults Whether or not to summarize provenance events returned. This property is false by default.
   */
  def getProvenance(id: String, clusterNodeId: Option[String] = None, summarize: Option[Boolean] = None, incrementalResults: Option[Boolean] = None): ApiRequestT[ProvenanceEntity] =
    basicRequest
      .method(Method.GET, uri"$baseUrl/provenance/${id}?clusterNodeId=$clusterNodeId&summarize=$summarize&incrementalResults=$incrementalResults")
      .contentType("application/json")
      .response(asJson[ProvenanceEntity])

  /**
   * Expected answers:
   *   code 200 : ProvenanceOptionsEntity (successful operation)
   *   code 400 :  (NiFi was unable to complete the request because it was invalid. The request should not be retried without modification.)
   *   code 401 :  (Client could not be authenticated.)
   *   code 403 :  (Client is not authorized to make this request.)
   *   code 409 :  (The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.)
   */
  def getSearchOptions(): ApiRequestT[ProvenanceOptionsEntity] =
    basicRequest
      .method(Method.GET, uri"$baseUrl/provenance/search-options")
      .contentType("application/json")
      .response(asJson[ProvenanceOptionsEntity])

  /**
   * Lineage queries may be long running so this endpoint submits a request. The response will include the current state of the query. If the request is not completed the URI in the response can be used at a later time to get the updated state of the query. Once the query has completed the lineage request should be deleted by the client who originally submitted it.
   * 
   * Expected answers:
   *   code 200 : LineageEntity (successful operation)
   *   code 400 :  (NiFi was unable to complete the request because it was invalid. The request should not be retried without modification.)
   *   code 401 :  (Client could not be authenticated.)
   *   code 403 :  (Client is not authorized to make this request.)
   *   code 404 :  (The specified resource could not be found.)
   *   code 409 :  (The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.)
   * 
   * @param body The lineage query details.
   */
  def submitLineageRequest(body: LineageEntity): ApiRequestT[LineageEntity] =
    basicRequest
      .method(Method.POST, uri"$baseUrl/provenance/lineage")
      .contentType("application/json")
      .body(body)
      .response(asJson[LineageEntity])

  /**
   * Provenance queries may be long running so this endpoint submits a request. The response will include the current state of the query. If the request is not completed the URI in the response can be used at a later time to get the updated state of the query. Once the query has completed the provenance request should be deleted by the client who originally submitted it.
   * 
   * Expected answers:
   *   code 200 : ProvenanceEntity (successful operation)
   *   code 400 :  (NiFi was unable to complete the request because it was invalid. The request should not be retried without modification.)
   *   code 401 :  (Client could not be authenticated.)
   *   code 403 :  (Client is not authorized to make this request.)
   *   code 409 :  (The request was valid but NiFi was not in the appropriate state to process it. Retrying the same request later may be successful.)
   * 
   * @param body The provenance query details.
   */
  def submitProvenanceRequest(body: ProvenanceEntity): ApiRequestT[ProvenanceEntity] =
    basicRequest
      .method(Method.POST, uri"$baseUrl/provenance")
      .contentType("application/json")
      .body(body)
      .response(asJson[ProvenanceEntity])

}

