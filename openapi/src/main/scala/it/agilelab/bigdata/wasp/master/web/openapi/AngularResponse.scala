package it.agilelab.bigdata.wasp.master.web.openapi

import it.agilelab.bigdata.wasp.master.web.openapi.ResultIndicator.ResultIndicator

case class AngularResponse[T](Result: ResultIndicator, data: T)
