package dev.linkedlogics.kafka.repository;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import dev.linkedlogics.LinkedLogics;
import dev.linkedlogics.config.LinkedLogicsConfiguration;

public class KafkaDataSource {
	private static final String KAFKA = "kafka";
	
	public Producer<String, String> getProducer() {
		Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConfig("bootstrap_servers").map(c -> c.toString()).orElseThrow(() -> new IllegalArgumentException("missing configuration " + KAFKA + ".bootstrap_servers")));
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
		return producer;
	}
	
	public Consumer<String, String> getConsumer() {
		Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaConfig("bootstrap_servers").map(c -> c.toString()).orElseThrow(() -> new IllegalArgumentException("missing configuration " + KAFKA + ".bootstrap_servers")));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(LinkedLogics.getApplicationName()));
        
        return consumer;
	}
	
	private static Optional<Object> getKafkaConfig(String config) {
		return LinkedLogicsConfiguration.getConfig(KAFKA + "." + config);
	}
}
