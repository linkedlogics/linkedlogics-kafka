package dev.linkedlogics.kafka;

import java.util.List;

import dev.linkedlogics.kafka.service.KafkaConsumerService;
import dev.linkedlogics.kafka.service.KafkaPublisherService;
import dev.linkedlogics.kafka.service.KafkaQueueService;
import dev.linkedlogics.service.LinkedLogicsService;
import dev.linkedlogics.service.ServiceProvider;
import dev.linkedlogics.service.local.QueueSchedulerService;

public class KafkaServices implements ServiceProvider {
	@Override
	public List<LinkedLogicsService> getMessagingServices() {
		return List.of(new KafkaQueueService(), new KafkaConsumerService(), new KafkaPublisherService());
	}

	@Override
	public List<LinkedLogicsService> getSchedulingServices() {
		return List.of(new QueueSchedulerService());
	}
}