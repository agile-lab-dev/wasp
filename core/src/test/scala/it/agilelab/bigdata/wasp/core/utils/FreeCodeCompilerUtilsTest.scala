package it.agilelab.bigdata.wasp.core.utils

import org.scalatest.{FlatSpec, Matchers}

class FreeCodeCompilerUtilsTest extends FlatSpec with Matchers {

  private val helper = FreeCodeCompilerUtilsDefault

  it should "test wrong code" in {
    val output = helper.validate(
      """val a = "banana"
        | a.test """.stripMargin)
    output.size shouldBe 1
    output.head.toString() should startWith("<virtual>:2")
  }


  it should "test complete code" in {
    val output = helper.validate(
      """val a = "banana"
        |a.toString()
        |val c = "bar" """.stripMargin)
    output.size shouldBe 0
  }

  it should "test complete code with warning" in {
    val output = helper.validate(
      """val a = "banana"
        |a
        |a
        |""".stripMargin)
    output.exists(_.errorType.equals("error")) shouldBe false
    output.size shouldBe 1
  }

  it should "test a strategy" in {
    val output = helper.validate("""val df = dataFrames.getFirstDataFrame.select("name","someNumber","someLong")
      |df.withColumn("someNumber",df("someNumber")*df("someNumber"))
      |  .withColumn("extra",lit("TEST"))
      |""".stripMargin)
    output.size shouldBe 0
  }


  it should "test a strategy wrong" in {
    val output = helper.validate("""val df = dataFrames.getFirstDataFrame.select("name","someNumber","someLong")
      |dfWrong.withColumn("someNumber",df("someNumber")*df("someNumber"))
      |.withColumn("extra",lit("TEST"))
      """.stripMargin)
    output.size shouldBe 1
    output.head.toString() should startWith ("<virtual>:2")
  }


  it should "test complete code 1 for a strategy" in {
    val output =  helper.complete(
      """val a = "banana"
        |a.""".stripMargin)
    val a = "banana"
    output.exists(m=> m.toComplete.equals("toInt")) shouldBe true
    output.exists(m=> m.toComplete.equals("zip")) shouldBe true

  }



  it should "test complete code 2  for a strategy" in {
    val output =  helper.complete(
      """val test = "banana"
        |val testi = "ciao"
        |val home = "home"
        |test.to""".stripMargin)
    output.exists(m=> m.toComplete.equals("toInt")) shouldBe true
    output.exists(m=> m.toComplete.equals("toString")) shouldBe true
    output.exists(m=> m.toComplete.equals("zip")) shouldBe false

  }


  it should "test complete code 3 for a strategy" in {
    val output =  helper.complete(
      """val test = "banana"
        |val test1 = "ciao"
        |val home = "home"
        |te""".stripMargin)
    output.exists(m=> m.toComplete.equals("test")) shouldBe true
    output.exists(m=> m.toComplete.equals("test1")) shouldBe true
    output.size shouldBe 2


  }

  it should "test complete code 4 for a strategy" in {
    val output =  helper.complete(
      """val test = "banana"
        |val test1 = "ciao"
        |val home = "home"
        |to""".stripMargin)
    output.exists(m=> m.toComplete.equals("test")) shouldBe false
    output.exists(m=> m.toComplete.equals("test1")) shouldBe false
    output.exists(m=> m.toComplete.equals("toString")) shouldBe true
    output.size shouldBe 1



  }


  it should "complete a strategy 1" in {
    val output =  helper.complete(
      """data""".stripMargin)
    println(output.mkString("\n"))
    output.exists(m=> m.toComplete.equals("test")) shouldBe false
    output.exists(m=> m.toComplete.equals("test1")) shouldBe false
    output.exists(m=> m.toComplete.equals("dataFrames")) shouldBe true
    output.size shouldBe 1

  }

  it should "complete a strategy 2" in {
    val output =  helper.complete(
      """dataFrames.he""".stripMargin)
    println(output.mkString("\n"))
    output.exists(m=> m.toComplete.equals("head")) shouldBe true
    output.exists(m=> m.toComplete.equals("headOption")) shouldBe true
    output.size shouldBe 2

  }


  it should "complete a strategy 3" in {
    val output =  helper.complete(
      """val df = dataFrames.getFirstDat""".stripMargin)
    println(output.mkString("\n"))
    output.exists(m=> m.toComplete.equals("getFirstDataFrame")) shouldBe true
    output.size shouldBe 1

  }

  it should "complete a strategy 4" in {
    val output =  helper.complete(
      """val df = dataFrames.getFirstDataFrame.select("name","someNumber","someLong")
        |df.""".stripMargin)
    println(output.mkString("\n"))
    output.exists(m=> m.toComplete.equals("select")) shouldBe true

  }

}