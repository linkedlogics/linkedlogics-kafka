package dev.linkedlogics.kafka.service;

import java.util.Optional;

import dev.linkedlogics.service.QueueService;

public class KafkaQueueService implements QueueService {

	@Override
	public void offer(String queue, String payload) {
		
	}

	@Override
	public Optional<String> poll(String queue) {
		return Optional.empty();
	}

}
