/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.kafka011

import java.io.{File, IOException}
import java.lang.{Integer => JInt}
import java.net.InetSocketAddress
import java.util.{Map => JMap, Properties, UUID}
import java.util.concurrent.TimeUnit

import scala.collection.JavaConverters._
import scala.language.postfixOps
import scala.util.Random

import kafka.admin.AdminUtils
import kafka.api.Request
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.utils.ZkUtils
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.{AdminClient, CreatePartitionsOptions, NewPartitions}
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.zookeeper.server.{NIOServerCnxnFactory, ZooKeeperServer}
import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.util.{ShutdownHookManager, Utils}

/**
 * This is a helper class for Kafka test suites. This has the functionality to set up
 * and tear down local Kafka servers, and to push data using Kafka producers.
 *
 * The reason to put Kafka test utility class in src is to test Python related Kafka APIs.
 */
class KafkaTestUtils(withBrokerProps: Map[String, Object] = Map.empty) extends Logging {

  // Zookeeper related configurations
  private val zkHost = "127.0.0.1"
  private var zkPort: Int = 0
  private val zkConnectionTimeout = 60000
  private val zkSessionTimeout = 10000

  private var zookeeper: EmbeddedZookeeper = _

  private var zkUtils: ZkUtils = _
  private var adminClient: AdminClient = null

  // Kafka broker related configurations
  private val brokerHost = "127.0.0.1"
  private var brokerPort = 0
  private var brokerConf: KafkaConfig = _

  // Kafka broker server
  private var server: KafkaServer = _

  // Kafka producer
  private var producer: Producer[String, String] = _

  // Flag to test whether the system is correctly started
  private var zkReady = false
  private var brokerReady = false
  private var leakDetector: AnyRef = null

  def zkAddress: String = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper address")
    s"$zkHost:$zkPort"
  }

  def brokerAddress: String = {
    assert(brokerReady, "Kafka not setup yet or already torn down, cannot get broker address")
    s"$brokerHost:$brokerPort"
  }

  def zookeeperClient: ZkUtils = {
    assert(zkReady, "Zookeeper not setup yet or already torn down, cannot get zookeeper client")
    Option(zkUtils).getOrElse(
      throw new IllegalStateException("Zookeeper client is not yet initialized"))
  }

  // Set up the Embedded Zookeeper server and get the proper Zookeeper port
  private def setupEmbeddedZookeeper(): Unit = {
    // Zookeeper server startup
    zookeeper = new EmbeddedZookeeper(s"$zkHost:$zkPort")
    // Get the actual zookeeper binding port
    zkPort = zookeeper.actualPort
    zkUtils = ZkUtils(s"$zkHost:$zkPort", zkSessionTimeout, zkConnectionTimeout, false)
    zkReady = true
  }

  // Set up the Embedded Kafka server
  private def setupEmbeddedKafkaServer(): Unit = {
    assert(zkReady, "Zookeeper should be set up beforehand")

    // Kafka broker startup
    Utils.startServiceOnPort(brokerPort, port => {
      brokerPort = port
      brokerConf = new KafkaConfig(brokerConfiguration, doLog = false)
      server = new KafkaServer(brokerConf)
      server.startup()
      brokerPort = server.boundPort(new ListenerName("PLAINTEXT"))
      (server, brokerPort)
    }, new SparkConf(), "KafkaBroker")

    brokerReady = true
    val props = new Properties()
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, s"$brokerHost:$brokerPort")
    adminClient = AdminClient.create(props)
  }

  /** setup the whole embedded servers, including Zookeeper and Kafka brokers */
  def setup(): Unit = {
    // Set up a KafkaTestUtils leak detector so that we can see where the leak KafkaTestUtils is
    // created.
    val exception = new SparkException("It was created at: ")
    leakDetector = ShutdownHookManager.addShutdownHook { () =>
      logError("Found a leak KafkaTestUtils.", exception)
    }

    setupEmbeddedZookeeper()
    setupEmbeddedKafkaServer()
    eventually(timeout(60.seconds)) {
      assert(zkUtils.getAllBrokersInCluster().nonEmpty, "Broker was not up in 60 seconds")
    }
  }

  /** Teardown the whole servers, including Kafka broker and Zookeeper */
  def teardown(): Unit = {
    if (leakDetector != null) {
      ShutdownHookManager.removeShutdownHook(leakDetector)
    }
    brokerReady = false
    zkReady = false

    if (producer != null) {
      producer.close()
      producer = null
    }

    if (adminClient != null) {
      adminClient.close()
    }

    if (server != null) {
      server.shutdown()
      server.awaitShutdown()
      server = null
    }

    // On Windows, `logDirs` is left open even after Kafka server above is completely shut down
    // in some cases. It leads to test failures on Windows if the directory deletion failure
    // throws an exception.
    brokerConf.logDirs.foreach { f =>
      try {
        Utils.deleteRecursively(new File(f))
      } catch {
        case e: IOException if Utils.isWindows =>
          logWarning(e.getMessage)
      }
    }

    if (zkUtils != null) {
      zkUtils.close()
      zkUtils = null
    }

    if (zookeeper != null) {
      zookeeper.shutdown()
      zookeeper = null
    }
  }

  /** Create a Kafka topic and wait until it is propagated to the whole cluster */
  def createTopic(topic: String, partitions: Int, overwrite: Boolean = false): Unit = {
    var created = false
    while (!created) {
      try {
        AdminUtils.createTopic(zkUtils, topic, partitions, 1)
        created = true
      } catch {
        // Workaround fact that TopicExistsException is in kafka.common in 0.10.0 and
        // org.apache.kafka.common.errors in 0.10.1 (!)
        case e: Exception if (e.getClass.getSimpleName == "TopicExistsException") && overwrite =>
          deleteTopic(topic)
      }
    }
    // wait until metadata is propagated
    (0 until partitions).foreach { p =>
      waitUntilMetadataIsPropagated(topic, p)
    }
  }

  def getAllTopicsAndPartitionSize(): Seq[(String, Int)] = {
    zkUtils.getPartitionsForTopics(zkUtils.getAllTopics()).mapValues(_.size).toSeq
  }

  /** Create a Kafka topic and wait until it is propagated to the whole cluster */
  def createTopic(topic: String): Unit = {
    createTopic(topic, 1)
  }

  /** Delete a Kafka topic and wait until it is propagated to the whole cluster */
  def deleteTopic(topic: String): Unit = {
    val partitions = zkUtils.getPartitionsForTopics(Seq(topic))(topic).size
    AdminUtils.deleteTopic(zkUtils, topic)
    verifyTopicDeletionWithRetries(zkUtils, topic, partitions, List(this.server))
  }

  /** Add new partitions to a Kafka topic */
  def addPartitions(topic: String, partitions: Int): Unit = {
    adminClient.createPartitions(
      Map(topic -> NewPartitions.increaseTo(partitions)).asJava,
      new CreatePartitionsOptions)
    // wait until metadata is propagated
    (0 until partitions).foreach { p =>
      waitUntilMetadataIsPropagated(topic, p)
    }
  }

  /** Java-friendly function for sending messages to the Kafka broker */
  def sendMessages(topic: String, messageToFreq: JMap[String, JInt]): Unit = {
    sendMessages(topic, Map(messageToFreq.asScala.mapValues(_.intValue()).toSeq: _*))
  }

  /** Send the messages to the Kafka broker */
  def sendMessages(topic: String, messageToFreq: Map[String, Int]): Unit = {
    val messages = messageToFreq.flatMap { case (s, freq) => Seq.fill(freq)(s) }.toArray
    sendMessages(topic, messages)
  }

  /** Send the array of messages to the Kafka broker */
  def sendMessages(topic: String, messages: Array[String]): Seq[(String, RecordMetadata)] = {
    sendMessages(topic, messages, None)
  }

  def sendMessages(records: Array[ProducerRecord[String, String]]): Seq[(String, RecordMetadata)] = {
    producer = new KafkaProducer[String, String](producerConfiguration)
    val offsets = try {
      records.map { record =>
        val m = record.value()
        val metadata =
          producer.send(record).get(10, TimeUnit.SECONDS)
        logInfo(s"\tSent $m to partition ${metadata.partition}, offset ${metadata.offset}")
        (m, metadata)
      }
    } finally {
      if (producer != null) {
        producer.close()
        producer = null
      }
    }
    offsets
  }

  /** Send the array of messages to the Kafka broker using specified partition */
  def sendMessages(
      topic: String,
      messages: Array[String],
      partition: Option[Int]): Seq[(String, RecordMetadata)] = {
    val records = messages.map { m =>
      partition match {
        case Some(p) => new ProducerRecord[String, String](topic, p, null, m)
        case None => new ProducerRecord[String, String](topic, m)
      }
    }
    sendMessages(records)
  }

  def cleanupLogs(): Unit = {
    server.logManager.cleanupLogs()
  }

  def getEarliestOffsets(topics: Set[String]): Map[TopicPartition, Long] = {
    val kc = new KafkaConsumer[String, String](consumerConfiguration)
    logInfo("Created consumer to get earliest offsets")
    kc.subscribe(topics.asJavaCollection)
    kc.poll(0)
    val partitions = kc.assignment()
    kc.pause(partitions)
    kc.seekToBeginning(partitions)
    val offsets = partitions.asScala.map(p => p -> kc.position(p)).toMap
    kc.close()
    logInfo("Closed consumer to get earliest offsets")
    offsets
  }

  def getLatestOffsets(topics: Set[String]): Map[TopicPartition, Long] = {
    val kc = new KafkaConsumer[String, String](consumerConfiguration)
    logInfo("Created consumer to get latest offsets")
    kc.subscribe(topics.asJavaCollection)
    kc.poll(0)
    val partitions = kc.assignment()
    kc.pause(partitions)
    kc.seekToEnd(partitions)
    val offsets = partitions.asScala.map(p => p -> kc.position(p)).toMap
    kc.close()
    logInfo("Closed consumer to get latest offsets")
    offsets
  }

  protected def brokerConfiguration: Properties = {
    val props = new Properties()
    props.put("broker.id", "0")
    props.put("host.name", "127.0.0.1")
    props.put("advertised.host.name", "127.0.0.1")
    props.put("port", brokerPort.toString)
    props.put("log.dir", Utils.createTempDir().getAbsolutePath)
    props.put("zookeeper.connect", zkAddress)
    props.put("zookeeper.connection.timeout.ms", "60000")
    props.put("log.flush.interval.messages", "1")
    props.put("replica.socket.timeout.ms", "1500")
    props.put("delete.topic.enable", "true")
    props.put("group.initial.rebalance.delay.ms", "10")

    // Change the following settings as we have only 1 broker
    props.put("offsets.topic.num.partitions", "1")
    props.put("offsets.topic.replication.factor", "1")
    props.put("transaction.state.log.replication.factor", "1")
    props.put("transaction.state.log.min.isr", "1")

    // Can not use properties.putAll(propsMap.asJava) in scala-2.12
    // See https://github.com/scala/bug/issues/10418
    withBrokerProps.foreach { case (k, v) => props.put(k, v) }
    props
  }

  private def producerConfiguration: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerAddress)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props.put("key.serializer", classOf[StringSerializer].getName)
    // wait for all in-sync replicas to ack sends
    props.put("acks", "all")
    props
  }

  /** Call `f` with a `KafkaProducer` that has initialized transactions. */
  def withTransactionalProducer(f: KafkaProducer[String, String] => Unit): Unit = {
    val props = producerConfiguration
    props.put("transactional.id", UUID.randomUUID().toString)
    val producer = new KafkaProducer[String, String](props)
    try {
      producer.initTransactions()
      f(producer)
    } finally {
      producer.close()
    }
  }

  private def consumerConfiguration: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerAddress)
    props.put("group.id", "group-KafkaTestUtils-" + Random.nextInt)
    props.put("value.deserializer", classOf[StringDeserializer].getName)
    props.put("key.deserializer", classOf[StringDeserializer].getName)
    props.put("enable.auto.commit", "false")
    props
  }

  /** Verify topic is deleted in all places, e.g, brokers, zookeeper. */
  private def verifyTopicDeletion(
      topic: String,
      numPartitions: Int,
      servers: Seq[KafkaServer]): Unit = {
    val topicAndPartitions = (0 until numPartitions).map(new TopicPartition(topic, _))

    import ZkUtils._
    // wait until admin path for delete topic is deleted, signaling completion of topic deletion
    assert(
      !zkUtils.pathExists(getDeleteTopicPath(topic)),
      s"${getDeleteTopicPath(topic)} still exists")
    assert(!zkUtils.pathExists(getTopicPath(topic)), s"${getTopicPath(topic)} still exists")
    // ensure that the topic-partition has been deleted from all brokers' replica managers
    assert(servers.forall(server => topicAndPartitions.forall(tp =>
      server.replicaManager.getPartition(tp) == None)),
      s"topic $topic still exists in the replica manager")
    // ensure that logs from all replicas are deleted if delete topic is marked successful
    assert(servers.forall(server => topicAndPartitions.forall(tp =>
      server.getLogManager().getLog(tp).isEmpty)),
      s"topic $topic still exists in log manager")
    // ensure that topic is removed from all cleaner offsets
    assert(servers.forall(server => topicAndPartitions.forall { tp =>
      val checkpoints = server.getLogManager().liveLogDirs.map { logDir =>
        new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint")).read()
      }
      checkpoints.forall(checkpointsPerLogDir => !checkpointsPerLogDir.contains(tp))
    }), s"checkpoint for topic $topic still exists")
    // ensure the topic is gone
    assert(
      !zkUtils.getAllTopics().contains(topic),
      s"topic $topic still exists on zookeeper")
  }

  /** Verify topic is deleted. Retry to delete the topic if not. */
  private def verifyTopicDeletionWithRetries(
      zkUtils: ZkUtils,
      topic: String,
      numPartitions: Int,
      servers: Seq[KafkaServer]) {
    eventually(timeout(60.seconds), interval(200.millis)) {
      try {
        verifyTopicDeletion(topic, numPartitions, servers)
      } catch {
        case e: Throwable =>
          // As pushing messages into Kafka updates Zookeeper asynchronously, there is a small
          // chance that a topic will be recreated after deletion due to the asynchronous update.
          // Hence, delete the topic and retry.
          AdminUtils.deleteTopic(zkUtils, topic)
          throw e
      }
    }
  }

  private def waitUntilMetadataIsPropagated(topic: String, partition: Int): Unit = {
    def isPropagated = server.metadataCache.getPartitionInfo(topic, partition) match {
      case Some(partitionState) =>
        zkUtils.getLeaderForPartition(topic, partition).isDefined &&
          Request.isValidBrokerId(partitionState.basePartitionState.leader) &&
          !partitionState.basePartitionState.replicas.isEmpty

      case _ =>
        false
    }
    eventually(timeout(60.seconds)) {
      assert(isPropagated, s"Partition [$topic, $partition] metadata not propagated after timeout")
    }
  }

  /**
   * Wait until the latest offset of the given `TopicPartition` is not less than `offset`.
   */
  def waitUntilOffsetAppears(topicPartition: TopicPartition, offset: Long): Unit = {
    eventually(timeout(60.seconds)) {
      val currentOffset = getLatestOffsets(Set(topicPartition.topic)).get(topicPartition)
      assert(currentOffset.nonEmpty && currentOffset.get >= offset)
    }
  }

  private class EmbeddedZookeeper(val zkConnect: String) {
    val snapshotDir = Utils.createTempDir()
    val logDir = Utils.createTempDir()

    val zookeeper = new ZooKeeperServer(snapshotDir, logDir, 500)
    val (ip, port) = {
      val splits = zkConnect.split(":")
      (splits(0), splits(1).toInt)
    }
    val factory = new NIOServerCnxnFactory()
    factory.configure(new InetSocketAddress(ip, port), 16)
    factory.startup(zookeeper)

    val actualPort = factory.getLocalPort

    def shutdown() {
      factory.shutdown()
      // The directories are not closed even if the ZooKeeper server is shut down.
      // Please see ZOOKEEPER-1844, which is fixed in 3.4.6+. It leads to test failures
      // on Windows if the directory deletion failure throws an exception.
      try {
        Utils.deleteRecursively(snapshotDir)
      } catch {
        case e: IOException if Utils.isWindows =>
          logWarning(e.getMessage)
      }
      try {
        Utils.deleteRecursively(logDir)
      } catch {
        case e: IOException if Utils.isWindows =>
          logWarning(e.getMessage)
      }
    }
  }
}

