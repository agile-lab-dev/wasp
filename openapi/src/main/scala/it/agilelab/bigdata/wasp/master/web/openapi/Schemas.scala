package it.agilelab.bigdata.wasp.master.web.openapi

import com.esotericsoftware.kryo.Kryo
import io.swagger.v3.oas.models.media.Schema

object Schemas {
  def copy(schema: Schema[_]): Schema[_] = {

    val kryo =  new Kryo
    kryo.copy(schema)

  }



}
