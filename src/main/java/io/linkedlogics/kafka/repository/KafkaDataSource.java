package io.linkedlogics.kafka.repository;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import io.linkedlogics.LinkedLogics;
import io.linkedlogics.config.LinkedLogicsConfiguration;

public class KafkaDataSource {
	private static final String KAFKA = "kafka";

	public Producer<String, String> getProducer() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConfig("bootstrap_servers").map(c -> c.toString()).orElseThrow(() -> new IllegalArgumentException("missing configuration " + KAFKA + ".bootstrap_servers")));
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<>(props);
		return producer;
	}

	public Consumer<String, String> getConsumer(String queue) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConfig("bootstrap_servers").map(c -> c.toString()).orElseThrow(() -> new IllegalArgumentException("missing configuration " + KAFKA + ".bootstrap_servers")));
		props.put(ConsumerConfig.GROUP_ID_CONFIG, LinkedLogics.getApplicationName());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
//		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(queue));
		return consumer;
	}

	private static Optional<Object> getKafkaConfig(String config) {
		return LinkedLogicsConfiguration.getConfig(KAFKA + "." + config);
	}
}
