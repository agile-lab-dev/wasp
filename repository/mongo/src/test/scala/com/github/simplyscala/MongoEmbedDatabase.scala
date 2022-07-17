package com.github.simplyscala

import de.flapdoodle.embed.mongo.distribution.Version
import de.flapdoodle.embed.mongo.{MongodExecutable, MongodProcess, MongodStarter}
import de.flapdoodle.embed.mongo.config.{Defaults, MongodConfig, Net}
import de.flapdoodle.embed.mongo.packageresolver.Command
import de.flapdoodle.embed.process.runtime.Network
import de.flapdoodle.embed.process.config.RuntimeConfig
import de.flapdoodle.embed.process.config.process.ProcessOutput


/**
  * Extends this trait provide to your test class a connection to embedMongo database
  */
trait MongoEmbedDatabase {

  private val runtimeConfig = Defaults.runtimeConfigFor(Command.MongoD)
    .processOutput(ProcessOutput.silent())
    .build()

  protected def mongoStart(port: Int = 12345,
                           host: String = "127.0.0.1",
                           version: Version = Version.V3_6_22,
                           runtimeConfig: RuntimeConfig = runtimeConfig): MongodProps = {
    val mongodExe: MongodExecutable = mongodExec(host, port, version, runtimeConfig)
    MongodProps(mongodExe.start(), mongodExe)
  }

  protected def mongoStop( mongodProps: MongodProps ) = {
    Option(mongodProps).foreach( _.mongodProcess.stop() )
    Option(mongodProps).foreach( _.mongodExe.stop() )
  }

  protected def withEmbedMongoFixture(port: Int = 12345,
                                      host: String = "127.0.0.1",
                                      version: Version = Version.V3_6_22,
                                      runtimeConfig: RuntimeConfig = runtimeConfig)
                                     (fixture: MongodProps => Any) {
    val mongodProps = mongoStart(port, host, version, runtimeConfig)
    try { fixture(mongodProps) } finally { Option(mongodProps).foreach( mongoStop ) }
  }

  private def runtime(config: RuntimeConfig): MongodStarter = MongodStarter.getInstance(config)

  private def mongodExec(host: String, port: Int, version: Version, runtimeConfig: RuntimeConfig): MongodExecutable =
    runtime(runtimeConfig).prepare(
      MongodConfig.builder()
        .version(version)
        .net(new Net(host, port, Network.localhostIsIPv6()))
        .build()
    )
}

sealed case class MongodProps(mongodProcess: MongodProcess, mongodExe: MongodExecutable)