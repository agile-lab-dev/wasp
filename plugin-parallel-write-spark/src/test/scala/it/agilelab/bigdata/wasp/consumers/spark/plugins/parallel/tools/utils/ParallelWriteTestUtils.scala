package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import com.squareup.okhttp.mockwebserver.{Dispatcher, MockWebServer}

import java.util.concurrent.CountDownLatch

object ParallelWriteTestUtils {

  case class ServerData(port: Int, latch: CountDownLatch, mockedServer: MockWebServer)

  def withServer[A](dispatcherFactory: CountDownLatch => Dispatcher, latchCount: Int = 1)(
    f: ServerData => A
  ): A = {
    val s = createAndStartServer(dispatcherFactory, latchCount)
    try f(s)
    finally s.mockedServer.shutdown()
  }

  def createAndStartServer(dispatcherFactory: CountDownLatch => Dispatcher, latchCount: Int): ServerData = {
    val port = 9999
    val latch = new CountDownLatch(latchCount)
    val mockedServer = new MockWebServer()
    mockedServer.setDispatcher(dispatcherFactory(latch))
    mockedServer.start(port)
    ServerData(port, latch, mockedServer)
  }

  def tapPrint[A](o: A, silent: Boolean = true): A = {
    if (!silent) {
      val s = o.toString
      val l = s.length
      val padding = 4
      println(">" * (l / 2 + padding / 2) + "<" * (l / 2 + padding / 2)) // scalastyle:ignore
      println("> " + s + " <") // scalastyle:ignore
      println(">" * (l / 2 + padding / 2) + "<" * (l / 2 + padding / 2)) // scalastyle:ignore
    }
    o
  }
}
