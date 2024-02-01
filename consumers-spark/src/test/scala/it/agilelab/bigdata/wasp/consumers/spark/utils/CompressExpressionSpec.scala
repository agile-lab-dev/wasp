package it.agilelab.bigdata.wasp.consumers.spark.utils

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util
import java.util.zip.{DeflaterOutputStream, GZIPOutputStream}

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream
import org.apache.hadoop.io.compress.DefaultCodec
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf.WHOLESTAGE_CODEGEN_ENABLED
import org.scalatest.{Assertion, FunSuite}
import org.xerial.snappy.SnappyOutputStream

import scala.util.Random


class CompressExpressionSpec extends FunSuite with SparkSuite {
  val colName = "value"
  def testProto(codec: String, r: Random, spark: SparkSession): Assertion = {
    import spark.implicits._
    val randomStrings = Seq.fill(100)(r.nextString(40).getBytes(StandardCharsets.UTF_8))
    val df            = spark.createDataset(randomStrings).select(col(colName).as(colName))
    val expected      = randomStrings.map(d => compress(codec, d))
    assert(
      df
        .select(CompressExpression.compress(col(colName), codec, spark.sparkContext.hadoopConfiguration))
        .as[Array[Byte]]
        .collect()
        .zip(expected)
        .forall(x => util.Arrays.equals(x._1, x._2))
    )
  }

  test("compress using gzip") {
    sessions.foreach(testProto("gzip", new Random(5), _))
  }

  test("compress using default") {
    sessions.foreach(testProto("default", new Random(5), _))
  }

  test("compress using deflate") {
    sessions.foreach(testProto("deflate", new Random(5), _))
  }

  ignore("compress using bz2") {
    sessions.foreach(testProto("bz2", new Random(5), _))
  }

  ignore("compress using snappy") {
    sessions.foreach(testProto("snappy", new Random(5), _))
  }

  private def sessions = List(
    {
      val ns = spark.newSession()
      ns.sql(s"set ${WHOLESTAGE_CODEGEN_ENABLED.key}=true")
      ns
    }, {
      val ns = spark.newSession()
      ns.sql(s"set ${WHOLESTAGE_CODEGEN_ENABLED.key}=false")
      ns
    }
  )

  def compress(codec: String, data: Array[Byte]): Array[Byte] =
    codec match {
      case "gzip"    =>
        val bos = new ByteArrayOutputStream()
        val gos = new GZIPOutputStream(bos)
        gos.write(data)
        gos.flush()
        gos.close()
        bos.toByteArray
      case "snappy"  =>
        val bos = new ByteArrayOutputStream()
        val gos = new SnappyOutputStream(bos)
        gos.write(data)
        gos.flush()
        gos.close()
        bos.toByteArray
      case "deflate" =>
        val bos = new ByteArrayOutputStream()
        val gos = new DeflaterOutputStream(bos)
        gos.write(data)
        gos.flush()
        gos.close()
        bos.toByteArray
      case "bz2"     =>
        val bos = new ByteArrayOutputStream()
        val gos = new BZip2CompressorOutputStream(bos)
        gos.write(data)
        gos.flush()
        gos.close()
        bos.toByteArray
      case "default" =>
        val codec = new DefaultCodec()
        codec.setConf(spark.sparkContext.hadoopConfiguration)
        val bos   = new ByteArrayOutputStream()
        val gos   = codec.createOutputStream(bos)
        gos.write(data)
        gos.flush()
        gos.close()
        bos.toByteArray
    }

}
