package io.linkedlogics.kafka.service;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.linkedlogics.service.QueueService;
import io.linkedlogics.kafka.repository.KafkaDataSource;

public class KafkaQueueService implements QueueService {
	private ConcurrentHashMap<String, Consumer<String, String>> consumers;
	private Producer<String, String> producer;
	private KafkaDataSource kafka;
	
	public KafkaQueueService() {
		kafka = new KafkaDataSource();
		producer = kafka.getProducer();
		consumers = new ConcurrentHashMap<>();
	}
	
	public void stop() {
		if (producer != null) {
			producer.close();
		}
		
		consumers.values().forEach(c -> c.close());
	}
	
	@Override
	public void offer(String queue, String payload) {
		ProducerRecord<String, String> record = new ProducerRecord<>(queue, payload);
        try {
			producer.send(record).get();
			producer.flush();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Optional<String> poll(String queue) {
		if (!consumers.containsKey(queue)) {
			consumers.putIfAbsent(queue, kafka.getConsumer(queue));
		}
	
		ConsumerRecords<String, String> records = consumers.get(queue).poll(Duration.ofMillis(2000));
		for (ConsumerRecord<String, String> record : records) {
			return Optional.of(record.value());
		}
		
		return Optional.empty();
	}
}
