package it.agilelab.bigdata.wasp.consumers.spark.plugins.parallel.tools.utils

import com.squareup.okhttp.mockwebserver.{Dispatcher, MockWebServer}

import java.util.concurrent.CountDownLatch

object ParallelWriteTestUtils {

  case class ServerData(port: Int, latch: CountDownLatch, mockedServer: MockWebServer)

  def withServer[A](dispatcherFactory: CountDownLatch => Dispatcher, latchCount: Int = 1)(
      f: ServerData => A
  ): A = {
    this.synchronized {
      val s = createAndStartServer(dispatcherFactory, latchCount)
      val oldPort = System.getProperty("wasp.test.mock.server.port")
      try {
        System.setProperty("wasp.test.mock.server.port", s.port.toString)
        f(s)
      } finally {
        if (oldPort != null) {
          System.setProperty("wasp.test.mock.server.port", oldPort)
        } else {
          System.clearProperty("wasp.test.mock.server.port")
        }
        s.mockedServer.shutdown()
      }
    }
  }

  def createAndStartServer(dispatcherFactory: CountDownLatch => Dispatcher, latchCount: Int): ServerData = {
    val latch        = new CountDownLatch(latchCount)
    val mockedServer = new MockWebServer()
    mockedServer.setDispatcher(dispatcherFactory(latch))
    mockedServer.start(0)
    val port         = mockedServer.getPort
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
}
