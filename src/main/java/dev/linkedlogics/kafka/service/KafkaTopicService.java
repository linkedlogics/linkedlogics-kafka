package dev.linkedlogics.kafka.service;

import java.util.Optional;

import dev.linkedlogics.service.TopicService;

public class KafkaTopicService implements TopicService {

	@Override
	public void offer(String queue, String payload) {
		
	}

	@Override
	public Optional<String> poll(String queue) {
		return Optional.empty();
	}
}
