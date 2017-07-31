package it.agilelab.bigdata.wasp.core.utils

import spray.json.JsValue

import scala.util.Try

/**
  * Created by Agile Lab s.r.l. on 23/08/2017.
  */
//http://limansky.me/posts/2016-04-30-easy-json-analyze-with-spray-json.html

object JsonOps {

  class JsFieldOps(val field: Option[JsValue]) {
    def \(name: String) = field map (_ \ name) getOrElse this

    def ===(x: JsValue) = field.contains(x)

    def =!=(x: JsValue) = !field.contains(x)
  }

  implicit class JsValueOps(val value: JsValue) {
    def \(name: String) = new JsFieldOps(Try(value.asJsObject).toOption.flatMap(_.fields.get(name)))
  }

}