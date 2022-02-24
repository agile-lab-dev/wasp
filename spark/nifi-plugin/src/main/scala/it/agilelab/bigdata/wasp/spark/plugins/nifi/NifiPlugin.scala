package it.agilelab.bigdata.wasp.spark.plugins.nifi

import java.io.File
import java.net.URLClassLoader
import java.nio.file.Paths
import java.util
import java.util.Collections
import java.util.concurrent.atomic.AtomicReference

import it.agilelab.bigdata.wasp.consumers.spark.strategies.{InternalStrategy, ReaderKey}
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import org.apache.hadoop.fs.{LocatedFileStatus, Path, RemoteIterator}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{ExecutorPlugin, SparkConf}
import org.slf4j.{Logger, LoggerFactory}

class NifiStrategy extends InternalStrategy {
  override def transform(dataFrames: Map[ReaderKey, DataFrame]): DataFrame = {

    import scala.collection.JavaConverters._

    val variables = new util.HashMap[String, String]

    configuration
      .getConfig("nifi.variables")
      .entrySet()
      .asScala
      .map(e => (e.getKey, e.getValue.render()))
      .toMap
      .foreach {
        case (k, v) =>
          variables.put(k, v)
      }

    val flowContent = configuration.getString("nifi.flow")

    val errorPort = configuration.getString("nifi.error-port")

    import org.apache.spark.sql.functions._

    val df = dataFrames.head._2

    val jsonDf = df.select(to_json(struct(col("*"))).as("json"))

    implicit val re: ExpressionEncoder[Row] = RowEncoder(jsonDf.schema)

    val recordSetDf = ArrayType(df.schema)

    jsonDf
      .mapPartitions { partition =>
        val flow = NifiPluginReference.instance.withClassloader(NifiPluginReference.instance.classLoader) {
          ReflectiveCall.flow(
            flowContent,
            util.Arrays.asList(errorPort),
            variables,
            NifiPluginReference.instance.extensionManager
          )
        }

        partition.map { row =>
          val input = row.getAs[String]("json")

          val output = NifiPluginReference.instance.withClassloader(NifiPluginReference.instance.classLoader) {
            ReflectiveCall.run(flow, input, Collections.emptyMap())
          }

          Row.apply(output)

        }

      }
      .select(from_json(col("json"), recordSetDf).as("data"))
      .select(explode(col("data")))
      .select(col("col.*"))

  }
}

object NifiPluginReference {

  private val plugin: AtomicReference[NifiPlugin] = new AtomicReference[NifiPlugin]()

  def instance: NifiPlugin = plugin.get()

  private[nifi] def install(plugin: NifiPlugin) = {
    this.plugin.compareAndSet(null, plugin)
  }

  private[nifi] def uninstall(plugin: NifiPlugin) = {
    this.plugin.compareAndSet(plugin, null)
  }

}

case class LibrariesConfiguration(stateless: Path, bootstrap: Path, system: Path, extensions: Path)

case class NifiPluginConfig(libraries: LibrariesConfiguration, hadoopConfiguration: HadoopConfiguration)

trait ConfigurationSupport {

  def parse(sparkConfig: SparkConf): NifiPluginConfig = {
    val hadoopConfig = SparkHadoopUtil.get.newConfiguration(sparkConfig)

    NifiPluginConfig(
      libraries = LibrariesConfiguration(
        new Path(sparkConfig.get("spark.wasp.nifi.lib.stateless", "")),
        new Path(sparkConfig.get("spark.wasp.nifi.lib.bootstrap", "")),
        new Path(sparkConfig.get("spark.wasp.nifi.lib.system", "")),
        new Path(sparkConfig.get("spark.wasp.nifi.lib.extensions", ""))
      ),
      hadoopConfiguration = hadoopConfig
    )

  }

}

trait FilesystemSupport {

  implicit def remoteIteratorToIterator[A](remote: RemoteIterator[A]): Iterator[A] = new Iterator[A] {
    override def hasNext: Boolean = remote.hasNext

    override def next(): A = remote.next()
  }

  def download(configuration: HadoopConfiguration, source: Path, destination: Path): Unit = {
    val fs = source.getFileSystem(configuration)

    list(configuration, source).foreach { status =>
      val path = new Path(status.getPath.toString.replace(source.toString, destination.toString))

      if (status.isDirectory) {
        destination.getFileSystem(configuration).mkdirs(path)
      } else {
        fs.copyToLocalFile(status.getPath, path)
      }

    }

  }

  def mkdir(configuration: HadoopConfiguration, path: Path): Boolean = {
    val fs = path.getFileSystem(configuration)
    fs.mkdirs(path)
  }

  def list(configuration: HadoopConfiguration, path: Path): RemoteIterator[LocatedFileStatus] = {
    val fs = path.getFileSystem(configuration)
    fs.listFiles(path, true)
  }
}

trait ExtensionManagerSupport {

  self: FilesystemSupport =>

  protected def createExtensionManager(
      hadoopConfiguration: HadoopConfiguration,
      stateless: Path,
      bootstrap: Path,
      system: Path,
      extensions: Path
  ): (ClassLoader, AnyRef) = {

    val bootstrapJars = list(hadoopConfiguration, bootstrap).map(_.getPath).map(_.toUri).map(_.toURL).toArray
    val systemJars    = list(hadoopConfiguration, system).map(_.getPath).map(_.toUri).map(_.toURL).toArray
    val statelessJars = list(hadoopConfiguration, stateless).map(_.getPath).map(_.toUri).map(_.toURL).toArray

    val statelessClassloader = new URLClassLoader(statelessJars, null)
    val bootstrapClassLoader = new URLClassLoader(bootstrapJars, statelessClassloader)
    val systemClassLoader    = new URLClassLoader(systemJars, bootstrapClassLoader)

    val extensionsAsFile = Paths.get(extensions.toUri).toFile

    withClassloader(statelessClassloader) {
      (statelessClassloader, ReflectiveCall.extensionManager(extensionsAsFile, systemClassLoader))
    }
  }

  def withClassloader[A](classLoader: ClassLoader)(f: => A): A = {

    val prev = Thread.currentThread().getContextClassLoader

    try {
      Thread.currentThread().setContextClassLoader(classLoader)
      f
    } finally {
      if (prev != null) {
        Thread.currentThread().setContextClassLoader(prev)
      }
    }

  }

}

class NifiPlugin(sparkConf: SparkConf)
    extends ExecutorPlugin
    with ConfigurationSupport
    with FilesystemSupport
    with ExtensionManagerSupport {

  lazy val log: Logger = LoggerFactory.getLogger(this.getClass)

  var extensionManager: AnyRef = _

  var classLoader: ClassLoader = _

  private val config: NifiPluginConfig = parse(sparkConf)

  override def init(): Unit = {
    NifiPluginReference.install(this)

    log.info("Loading NifiPlugin with configuration {}", config)

    val NifiPluginConfig(LibrariesConfiguration(stateless, bootstrap, system, extensions), hadoopConfiguration) = config

    val executorWorkingDir = new File(".").getAbsolutePath

    val destinationPath       = new Path(s"file://${executorWorkingDir}", "nifi")
    val bootstrapDestination  = new Path(destinationPath, "bootstrap")
    val systemDestination     = new Path(destinationPath, "system")
    val statelessDestination  = new Path(destinationPath, "stateless")
    val extensionsDestination = new Path(destinationPath, "extensions")
    mkdir(hadoopConfiguration, destinationPath)
    mkdir(hadoopConfiguration, bootstrapDestination)
    mkdir(hadoopConfiguration, systemDestination)

    log.info("Downloading {} to {}", Array(bootstrap, bootstrapDestination))
    download(hadoopConfiguration, bootstrap, bootstrapDestination)
    log.info("Downloading {} to {}", Array(system, systemDestination))
    download(hadoopConfiguration, system, systemDestination)
    log.info("Downloading {} to {}", Array(stateless, statelessDestination))
    download(hadoopConfiguration, stateless, statelessDestination)
    log.info("Downloading {} to {}", Array(extensions, extensionsDestination))

    download(hadoopConfiguration, extensions, extensionsDestination)

    val result = createExtensionManager(
      hadoopConfiguration,
      statelessDestination,
      bootstrapDestination,
      systemDestination,
      extensionsDestination
    )

    classLoader = result._1
    extensionManager = result._2

  }

  override def shutdown(): Unit = {
    NifiPluginReference.uninstall(this)
  }

}
