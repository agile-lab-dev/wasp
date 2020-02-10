package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.sql.{Date, Timestamp}
import java.time.{LocalDate, ZoneId, ZoneOffset}
import java.util.concurrent.atomic.AtomicLong

import scala.util.Random

object RowToAvroExpressionTestDataGenerator {


  def generate(seed: Long, elements: Int): Seq[UglyCaseClass] = {

    //random number generator with fixed seed to have reproducible tests
    val random = new Random(seed)

    Iterator.continually {
      generate(random)
    }.take(elements).toSeq
  }

  def generate(random: Random): UglyCaseClass = {

    UglyCaseClass(generateByteArray(random),
      generateIntArray(random),
      Iterator.continually(generateNestedCaseClass(random)).take(10).toArray,
      generateDate(random),
      generateTimestamp(random),
      generateNestedCaseClass(random),
      Iterator.continually(random.nextString(3) -> random.nextInt()).take(10).toMap,
      generateMapOfOptionDouble(random, size = 5),
      Iterator.continually(random.nextString(3) -> generateMapOfOptionDouble(random, size = 5)).take(10).toMap,
      Iterator.continually(generateNestedCaseClass(random)).take(10).map {
        random.nextString(5) -> _
      }.toMap
    )

  }

  def generateMapOfOptionDouble(random: Random, size: Int): Map[String, Option[Double]] = {
    Iterator.continually(random.nextBoolean()).take(size).map(i => if (i) Some(random.nextDouble()) else None).map(
      Random.nextString(5) -> _
    ).toMap
  }

  def generateByteArray(random: Random): Array[Byte] = {
    val array = new Array[Byte](25)
    random.nextBytes(array)
    array
  }

  def generateIntArray(random: Random): Array[Int] = {
    Iterator.continually(random.nextInt()).take(10).toArray
  }


  val atomicCounter = new AtomicLong()

  def generateTimestamp(random: Random): Timestamp = {

    //spark will store timestamp as a long but as microsecond values instead of milliseconds
    //multiplying by 1000 and dividing by 1000 will result in a loss of ~ 10 bit of precision
    //we are performing a modulo operation over the returned long from random.nextLong to
    //generate a non uniform distribution (but close enough) not exceeding the precision of
    // internal spark computation
    //
    // BEWARE SPARK WILL NOT RAISE AN EXCEPTION, DATA WILL BE SILENTLY CORRUPTED
    val maxSparkSupportedPrecisionForTimestamp: Long = 1L << 53

    val millis = random.nextLong() % maxSparkSupportedPrecisionForTimestamp

    new Timestamp(millis)
  }

  def generateDate(random: Random): Date = {

    val maxSparkSupportedPrecisionForDate: Long = 1 << 30

    Date.valueOf(LocalDate.ofEpochDay(random.nextInt() % maxSparkSupportedPrecisionForDate))
  }

  def generateNestedCaseClass(random: Random): NestedCaseClass =
    NestedCaseClass(random.nextDouble(), random.nextLong(), random.nextString(30))


  def generateMap(random: Random): Map[String, Int] =
    Iterator.continually {
      (random.nextString(20), random.nextInt())
    }.take(20).toMap
}
