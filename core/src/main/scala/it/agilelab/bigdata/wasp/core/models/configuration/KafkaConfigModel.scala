package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.utils.ConnectionConfig

case class KafkaConfigModel(connections: Seq[ConnectionConfig],
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

	def ingestRateToMills() = {
		val defaultIngestRate = 1000
		try {
			this.ingest_rate.replace("s", "").toInt * defaultIngestRate
		} catch {
			case _ : Throwable => defaultIngestRate
		}
	}

}

case class TinyKafkaConfig(connections: Seq[ConnectionConfig],
                           batch_send_size: Int,
                           default_encoder: String,
                           encoder_fqcn: String,
                           partitioner_fqcn: String)