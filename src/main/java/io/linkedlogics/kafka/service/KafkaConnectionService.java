package io.linkedlogics.kafka.service;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import io.linkedlogics.LinkedLogics;
import io.linkedlogics.kafka.service.config.KafkaConnectionServiceConfig;
import io.linkedlogics.service.LinkedLogicsService;
import io.linkedlogics.service.config.ServiceConfiguration;

public class KafkaConnectionService implements LinkedLogicsService {
	
	private KafkaConnectionServiceConfig config = new ServiceConfiguration().getConfig(KafkaConnectionServiceConfig.class);
	
	public Producer<String, String> getProducer() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
		props.put(ProducerConfig.ACKS_CONFIG, config.getAckConfig("all"));
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<>(props);
		return producer;
	}

	public Consumer<String, String> getConsumer(String queue) {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServers());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, LinkedLogics.getApplicationName());
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		config.getMaxPollCount().ifPresent(c -> {
			props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, c);
		});

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(queue));
		return consumer;
	}
}
