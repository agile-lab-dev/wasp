package it.agilelab.bigdata.wasp.spark.plugins.telemetry

case class TelemetryPluginKafkaConfig(connections: Seq[TelemetryPluginConnectionConfig],
                                      batch_send_size: Int,
                                      acks: String,
                                      default_encoder: String,
                                      encoder_fqcn: String,
                                      partitioner_fqcn: String,
                                      others: Seq[(String, String)])
