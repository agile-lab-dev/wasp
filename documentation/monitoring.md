# Monitoring

The most customizable way of monitoring wasp data processing pipelines is subscribing an actor to Telemetry Actor.

Sometimes that might be an overkill, therefore, two abstract kind of producers have been implemented for users to 
extend in order to obtain simple metrics about the data processing pipelines:

- BacklogSizeAnalyzer: outputs the size of the backlog of a Spark ETL 
- KafkaThroughput: outputs how many messages have been enqueued on a kafka topic in the last X ms

Both these features are available through abstract producers that should be extended by the user. 
Metrics can only be pushed to a Kafka topic. In order to make both kind of producers work, it is needed
that `KafkaCheckOffsetsGuardian` actor is started in the Wasp Producer JVM like the following:

```scala
WaspSystem.actorSystem.actorOf(
      KafkaCheckOffsetsGuardian.props(ConfigManager.getKafkaConfig),
      KafkaCheckOffsetsGuardian.name
    )
```

## BacklogSizeAnalyzer

In order to produce backlog metrics about one or more pipegraphs you should implement 
a concrete `BacklogSizeAnalyzerProducerActor` and a `BacklogSizeAnalyzerProducerGuardian`, both are generic in the type
of message that you want to output to the metric kafka topic

```scala
case class Metric(metricName: String, value: Long)
object Metric {
  def fromBacklogInfo(i: BacklogInfo): Metric = Metric(i.eltName + ":" + i.topicName, i.backlogSize)
}

class ImplBacklogSizeAnalyzerProducerGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL},
                                              producerName: String) extends BacklogSizeAnalyzerProducerGuardian[Metric](env, producerName) {
  override protected def createActor(kafka_router: ActorRef, kafkaOffsetChecker: ActorRef, topic: Option[TopicModel], topicToCheck: String, etlName: String): BacklogSizeAnalyzerProducerActor[String] = {
    new ImplBacklogSizeAnalyzerProducerActor(kafka_router, kafkaOffsetChecker, topic, topicToCheck, etlName)
  }
}

class ImplBacklogSizeAnalyzerProducerActor(kafka_router: ActorRef,
                                           kafkaOffsetChecker: ActorRef,
                                           topic: Option[TopicModel],
                                           topicToCheck: String,
                                           etlName: String
                                          ) extends BacklogSizeAnalyzerProducerActor[Metric](kafka_router, kafkaOffsetChecker, topic, topicToCheck, etlName) {
  override def toFinalMessage(i: BacklogInfo): Metric = Metric.fromBacklogInfo(i)

  override def generateOutputJsonMessage(input: Metric): String = input.toJson.toString

  override def retrievePartitionKey: Metric => String = _.etlName
}
```

By default the `BacklogSizeAnalyzerProducerGuardian` will read the following configuration (example):

```hocon
wasp.backlogSizeAnalyzer {
  pipegraphs = [{
    pipegraphName = "TestConsoleWriterStructuredJSONPipegraph"
  }]
}
```

This will have the effect of monitoring all the ETL of `TestConsoleWriterStructuredJSONPipegraph`. 
It is possible to override this behaviour implementing: `BacklogSizeAnalyzerProducerGuardian.backlogAnalyzerConfigs`

As for every Wasp producer™ you need to create its related model, insert it into WaspDB™ and start it through the 
standard REST api.

## KafkaThroughput

In order to produce throughput metrics about one or more kafka topics you should implement 
a concrete `KafkaThroughputProducerActor` and a `KafkaThroughputProducerGuardian`, both are generic in the type
of message that you want to output to the metric kafka topic

```scala
case class Metric(metricName: String, value: Long)

class ImplKafkaThroughputProducer(kafka_router: ActorRef,
                                  kafkaOffsetChecker: ActorRef,
                                  topic: Option[TopicModel],
                                  topicToCheck: String,
                                  windowSize: Long,
                                  sendMessageEveryXsamples: Int,
                                  triggerIntervalMs: Long) extends KafkaThroughputProducerActor[MetricInfo](
  kafka_router, kafkaOffsetChecker, topic, topicToCheck, windowSize, sendMessageEveryXsamples, triggerIntervalMs) {

  override protected def toFinalMessage(messageSumInWindow: Long, timestamp: Long): MetricInfo =
    MetricInfo(topicToCheck + "-" + messageSumInWindow)
  
  override def generateOutputJsonMessage(input: MetricInfo): String = input.toJson.toString

  override def retrievePartitionKey: MetricInfo => String = _.metricName
}

class ImplKafkaThroughputGuardian(env: {val producerBL: ProducerBL; val topicBL: TopicBL}, producerName: String) extends KafkaThroughputProducerGuardian[MetricInfo](env, producerName) {

  override protected def createActor(kafkaActor: ActorRef, topicToCheck: String, triggerInterval: Long, windowSize: Long, sendMessageEveryXsamples: Int): KafkaThroughputProducerActor[MetricInfo] = {
    new ImplKafkaThroughputProducer(kafka_router, kafkaActor, associatedTopic, topicToCheck, windowSize, sendMessageEveryXsamples, triggerInterval)
  }
}
```

By default the `KafkaThroughputProducerGuardian` will read the following configuration (example):

```hocon
kafkaThroughput {
  topics = [{
    topicName: "test_json.topic"
    triggerIntervalMs: 1000
    windowSizeMs: 5000
    sendMessageEvery: 1
  }]
}
```

This will have the effect of monitoring the kafka topics which have "topicName", kafka will be contacted 
every "triggerIntervalMs", the metric will output how many messages have been enqueued on the kafka topic in the last 
"windowSizeMs", a message will be sent on kafka every "sendMessageEvery" checks on kafka.

For example with this configuration:

- topicName: "test_json.topic"
- triggerIntervalMs: 2000
- windowSizeMs: 60000
- sendMessageEvery: 10

Every 2 seconds the producer will contact Kafka to know the latest offsets of topic `test_json.topic`, the output will 
contain the sum of all messages enqueued on `test_json.topic` in the last 60 seconds, a message will be sent on Kafka
output topic every 20 seconds (i.e. 2 * 10 seconds). 

It is possible to override this behaviour implementing: `KafkaThroughputProducerGuardian.kafkaThroughputConfigs`.

As for every Wasp producer™ you need to create its related model, insert it into WaspDB™ and start it through the 
standard REST api.
