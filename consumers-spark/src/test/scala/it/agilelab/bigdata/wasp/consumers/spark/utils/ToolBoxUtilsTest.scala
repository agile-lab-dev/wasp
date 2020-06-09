package it.agilelab.bigdata.wasp.consumers.spark.utils

import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random



class ToolBoxUtilsTest extends FlatSpec with Matchers {

  it should "test compileCode for a value" in {
    val valueInt = 1
    val int: Int = ToolBoxUtils.compileCode[Int](s"$valueInt")
    int.isInstanceOf[Int] shouldBe true
    int shouldBe 1

    val valueString = "Hello"
    val string = ToolBoxUtils.compileCode[String](s""""$valueString"""")
    string.isInstanceOf[String] shouldBe true
    string shouldBe valueString

    val valueBoolean = true
    val boolean = ToolBoxUtils.compileCode[Boolean](valueBoolean.toString)
    boolean.isInstanceOf[Boolean] shouldBe true
    boolean shouldBe valueBoolean

  }

  it should "test compileCode for a function" in {
    val f1 = ToolBoxUtils.compileCode[Int => Int]("(i:Int) => i+1")
    f1.isInstanceOf[Function1[Int, Int]] shouldBe true
    val expectedFunction1 = (i: Int) => i + 1
    for (_ <- 1 to 10) {
      val valueInt = Random.nextInt()
      f1(valueInt) shouldBe expectedFunction1(valueInt)
    }

    val f2 = ToolBoxUtils.compileCode[Int => (String, Int, Double)]("""(i:Int) => (s"$i",i,i.toDouble)""")
    f2.isInstanceOf[Function1[Int, (String, Int, Double)]] shouldBe true
    val expectedFunction2 = (i: Int) => (s"$i", i, i.toDouble)
    for (_ <- 1 to 10) {
      val valueInt = Random.nextInt()
      f2(valueInt) shouldBe expectedFunction2(valueInt)
    }

  }

  it should "test compileCode for a class" in {

    val clazzValue =
      """
        |
        |class Test() extends Tuple1 {
        |      override def toString(): String = "TEST"
        |}
        |
        |scala.reflect.classTag[Test].runtimeClass
    """.stripMargin

    val clazz = ToolBoxUtils.compileCode[Class[Tuple1[_]]](clazzValue)
    val instance = clazz.getConstructor().newInstance()
    instance.toString shouldBe "TEST"

  }






}
