package it.agilelab.bigdata.wasp.consumers.spark.http.utils

case class FormattedPathParams(
                                varKey: String,
                                valueKey: String
                              ) {
  def formattedVarKey: String = "${"+varKey+"}"
}