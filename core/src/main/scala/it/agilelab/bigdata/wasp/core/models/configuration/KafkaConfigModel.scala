package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.utils.ConnectionConfig

case class KafkaConfigModel(connections: Array[ConnectionConfig],
                            ingest_rate: String,
	                          zookeeper: ConnectionConfig,
	                          broker_id: String,
	                          partitioner_fqcn: String,
	                          default_encoder: String,
	                          encoder_fqcn: String,
	                          decoder_fqcn: String,
	                          batch_send_size: Int,
	                          name: String) {

	def toTinyConfig() = TinyKafkaConfig(connections, batch_send_size, default_encoder, encoder_fqcn, partitioner_fqcn)

}

case class TinyKafkaConfig(connections: Array[ConnectionConfig],
                           batch_send_size: Int,
                           default_encoder: String,
                           encoder_fqcn: String,
                           partitioner_fqcn: String)