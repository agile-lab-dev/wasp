package it.agilelab.bigdata.wasp.core.models.configuration

import it.agilelab.bigdata.wasp.core.models.Model
import it.agilelab.bigdata.wasp.core.utils.{ConnectionConfig, ZookeeperConnectionsConfig}

case class KafkaConfigModel(connections: Seq[ConnectionConfig],
														ingest_rate: String,
														zookeeperConnections: ZookeeperConnectionsConfig,
														broker_id: String,
														partitioner_fqcn: String,
														default_encoder: String,
														key_encoder_fqcn: String,
														encoder_fqcn: String,
														decoder_fqcn: String,
														batch_send_size: Int,
														others: Seq[KafkaEntryConfig],
														name: String
                           ) extends Model {

	def toTinyConfig() = TinyKafkaConfig(connections, batch_send_size, default_encoder, encoder_fqcn, partitioner_fqcn, others)

	def ingestRateToMills() = {
		val defaultIngestRate = 1000
		try {
			this.ingest_rate.replace("s", "").toInt * defaultIngestRate
		} catch {
			case _ : Throwable => defaultIngestRate
		}
	}
}

case class TinyKafkaConfig(
														connections: Seq[ConnectionConfig],
														batch_send_size: Int,
														default_encoder: String,
														encoder_fqcn: String,
														partitioner_fqcn: String,
														others: Seq[KafkaEntryConfig])

case class KafkaEntryConfig(
														 key: String,
														 value: String
													 ) {
	def toTupla = (key, value)
}