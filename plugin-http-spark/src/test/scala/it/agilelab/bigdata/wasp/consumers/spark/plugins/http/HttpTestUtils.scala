package it.agilelab.bigdata.wasp.consumers.spark.plugins.http

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.ServerSocket
import java.util.concurrent.CountDownLatch
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import com.squareup.okhttp.mockwebserver.{Dispatcher, MockWebServer}
import it.agilelab.bigdata.wasp.core.utils.Utils.using
import org.apache.commons.io.IOUtils

object HttpTestUtils {

  case class ServerData(port: Int, latch: CountDownLatch, mockedServer: MockWebServer)

  def getAvailablePort: Int = using(new ServerSocket(0))(_.getLocalPort)

  def withServer[A](dispatcherFactory: CountDownLatch => Dispatcher, latchCount: Int = 1)(
    f: ServerData => A
  ): A = {
    val s = createAndStartServer(dispatcherFactory, latchCount)
    try f(s)
    finally s.mockedServer.shutdown()
  }

  def createAndStartServer(dispatcherFactory: CountDownLatch => Dispatcher, latchCount: Int): ServerData = {
    val port         = getAvailablePort
    val latch        = new CountDownLatch(latchCount)
    val mockedServer = new MockWebServer()
    mockedServer.setDispatcher(dispatcherFactory(latch))
    mockedServer.start(port)
    ServerData(port, latch, mockedServer)
  }

  def tapPrint[A](o: A, silent: Boolean = true): A = {
    if (!silent) {
      val s       = o.toString
      val l       = s.length
      val padding = 4
      println(">" * (l / 2 + padding / 2) + "<" * (l / 2 + padding / 2)) // scalastyle:ignore
      println("> " + s + " <")                                           // scalastyle:ignore
      println(">" * (l / 2 + padding / 2) + "<" * (l / 2 + padding / 2)) // scalastyle:ignore
    }
    o
  }

  def compress(bytes: Array[Byte]): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val gos = new GZIPOutputStream(bos)
    gos.write(bytes)
    gos.flush()
    gos.close()
    bos.toByteArray
  }

  def decompress(bytes: Array[Byte]): Array[Byte] =
    using(new GZIPInputStream(new ByteArrayInputStream(bytes))) { gis =>
      IOUtils.toByteArray(gis)
    }
}
