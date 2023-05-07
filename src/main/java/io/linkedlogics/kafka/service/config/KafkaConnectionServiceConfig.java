package io.linkedlogics.kafka.service.config;

import java.util.Optional;

import io.linkedlogics.service.config.Config;
import io.linkedlogics.service.config.Prefix;

@Prefix("kafka")
public interface KafkaConnectionServiceConfig {

	@Config(key = "bootstrap-servers", description = "Kafka bootstrap server list", required = true)
	public Object getBootstrapServers();
	
	@Config(key = "producer.ack", description = "Kafka ACK config")
	public String getAckConfig(String defaultValue);
	
	@Config(key = "consumer.max-poll-count", description = "Kafka Max poll count")
	public Optional<Integer> getMaxPollCount();
}
