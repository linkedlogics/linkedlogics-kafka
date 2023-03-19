package dev.linkedlogics.kafka.service;

import dev.linkedlogics.service.ServiceConfigurer;
import dev.linkedlogics.service.local.QueueSchedulerService;

public class KafkaServiceConfigurer extends ServiceConfigurer {
	public KafkaServiceConfigurer() {
		configure(new KafkaQueueService());
		configure(new KafkaTopicService());
		configure(new QueueSchedulerService());
	}
}

