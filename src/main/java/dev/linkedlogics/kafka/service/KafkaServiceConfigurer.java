package dev.linkedlogics.kafka.service;

import dev.linkedlogics.service.ServiceConfigurer;

public class KafkaServiceConfigurer extends ServiceConfigurer {
	public KafkaServiceConfigurer() {
		configure(new KafkaQueueService());
		configure(new KafkaConsumerService());
		configure(new KafkaPublisherService());
	}
}

