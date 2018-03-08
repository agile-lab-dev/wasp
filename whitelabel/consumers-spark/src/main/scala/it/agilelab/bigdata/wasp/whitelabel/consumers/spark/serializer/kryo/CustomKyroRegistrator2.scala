package it.agilelab.bigdata.wasp.whitelabel.consumers.spark.serializer.kryo

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator

class CustomKyroRegistrator2 extends KryoRegistrator {

  override def registerClasses(kryo: Kryo): Unit = {
    //println("CustomKyroRegistrator2")

    /* Examples */
    //kryo.register(classOf[Array[Array[Byte]]])
    //kryo.register(classOf[scala.collection.mutable.WrappedArray.ofRef[_]])
    //kryo.register(classOf[Array[org.apache.spark.sql.Row]])
  }
}
