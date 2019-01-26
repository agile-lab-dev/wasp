package it.agilelab.bigdata.wasp.core.models.builder

import org.scalatest.{FlatSpec, Matchers}


class KVColumnSpec extends FlatSpec with Matchers {

  it should "generate it correctly" in {
    assert(KVColumn.primitive((x: BigClass) => x.aString, "ciao") ===
      KVColumn.primitive("aString", "ciao", KVType.STRING))
    assertDoesNotCompile(
      """KVColumn.primitive((x: BigClass) => x.arrString, "ciao")"""
    )
  }

  it should "generate correct KVColumn of aInt" in {
    assert(KVColumn.primitive((x: BigClass) => x.aInt, "aaa") ===
      KVColumn.primitive("aInt", "aaa", "Int"))
  }
  it should "generate correct KVColumn of oInt" in {
    assert(KVColumn.primitive((x: BigClass) => x.oInt, "aaa") ===
      KVColumn.primitive("oInt", "aaa", "Int"))
  }
  it should "generate correct KVColumn of aLong" in {
    assert(KVColumn.primitive((x: BigClass) => x.aLong, "aaa") ===
      KVColumn.primitive("aLong", "aaa", "Long"))
  }
  it should "generate correct KVColumn of oLong" in {
    assert(KVColumn.primitive((x: BigClass) => x.oLong, "aaa") ===
      KVColumn.primitive("oLong", "aaa", "Long"))
  }
  it should "generate correct KVColumn of aString" in {
    assert(KVColumn.primitive((x: BigClass) => x.aString, "aaa") ===
      KVColumn.primitive("aString", "aaa", "String"))
  }
  it should "generate correct KVColumn of oString" in {
    assert(KVColumn.primitive((x: BigClass) => x.oString, "aaa") ===
      KVColumn.primitive("oString", "aaa", "String"))
  }
  it should "generate correct KVColumn of aBoolean" in {
    assert(KVColumn.primitive((x: BigClass) => x.aBoolean, "aaa") ===
      KVColumn.primitive("aBoolean", "aaa", "Boolean"))
  }
  it should "generate correct KVColumn of oBoolean" in {
    assert(KVColumn.primitive((x: BigClass) => x.oBoolean, "aaa") ===
      KVColumn.primitive("oBoolean", "aaa", "Boolean"))
  }
  it should "generate correct KVColumn of aByte" in {
    assert(KVColumn.primitive((x: BigClass) => x.aByte, "aaa") ===
      KVColumn.primitive("aByte", "aaa", "Byte"))
  }
  it should "generate correct KVColumn of oByte" in {
    assert(KVColumn.primitive((x: BigClass) => x.oByte, "aaa") ===
      KVColumn.primitive("oByte", "aaa", "Byte"))
  }
  it should "generate correct KVColumn of aBytes" in {
    assert(KVColumn.primitive((x: BigClass) => x.aBytes, "aaa") ===
      KVColumn.primitive("aBytes", "aaa", "Bytes"))
  }
  it should "generate correct KVColumn of oBytes" in {
    assert(KVColumn.primitive((x: BigClass) => x.oBytes, "aaa") ===
      KVColumn.primitive("oBytes", "aaa", "Bytes"))
  }
  it should "generate correct KVColumn of aDouble" in {
    assert(KVColumn.primitive((x: BigClass) => x.aDouble, "aaa") ===
      KVColumn.primitive("aDouble", "aaa", "Double"))
  }
  it should "generate correct KVColumn of oDouble" in {
    assert(KVColumn.primitive((x: BigClass) => x.oDouble, "aaa") ===
      KVColumn.primitive("oDouble", "aaa", "Double"))
  }
  it should "generate correct KVColumn of aFloat" in {
    assert(KVColumn.primitive((x: BigClass) => x.aFloat, "aaa") ===
      KVColumn.primitive("aFloat", "aaa", "Float"))
  }
  it should "generate correct KVColumn of oFloat" in {
    assert(KVColumn.primitive((x: BigClass) => x.oFloat, "aaa") ===
      KVColumn.primitive("oFloat", "aaa", "Float"))
  }
  it should "generate correct KVColumn of aShort" in {
    assert(KVColumn.primitive((x: BigClass) => x.aShort, "aaa") ===
      KVColumn.primitive("aShort", "aaa", "Short"))
  }
  it should "generate correct KVColumn of oShort" in {
    assert(KVColumn.primitive((x: BigClass) => x.oShort, "aaa") ===
      KVColumn.primitive("oShort", "aaa", "Short"))
  }
  it should "not generate KVColumn of Array[String]" in {
    assertDoesNotCompile(
      """KVColumn.primitive((x: BigClass) => x.arrString, "aaa") ===
      KVColumn.primitive("arrString", "aaa", "Bytes")""")
  }

}

case class BigClass(aInt: Int,
                    oInt: Option[Int],
                    aLong: Long,
                    oLong: Option[Long],
                    aString: String,
                    oString: Option[String],
                    aBoolean: Boolean,
                    oBoolean: Option[Boolean],
                    aByte: Byte,
                    oByte: Option[Byte],
                    aBytes: Array[Byte],
                    oBytes: Option[Array[Byte]],
                    aDouble: Double,
                    oDouble: Option[Double],
                    aFloat: Float,
                    oFloat: Option[Float],
                    aShort: Short,
                    oShort: Option[Short],
                    arrString: Array[String])
