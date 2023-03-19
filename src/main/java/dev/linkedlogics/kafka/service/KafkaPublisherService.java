package dev.linkedlogics.kafka.service;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.linkedlogics.context.Context;
import dev.linkedlogics.kafka.repository.KafkaDataSource;
import dev.linkedlogics.service.PublisherService;
import dev.linkedlogics.service.ServiceLocator;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaPublisherService implements PublisherService {

	private Producer<String, String> kafkaproducer;
	
	public KafkaPublisherService() {
		kafkaproducer = new KafkaDataSource().getProducer();
	}
	
	public void stop() {
		if (kafkaproducer != null) {
			kafkaproducer.close();
		}
	}

	@Override
	public void publish(Context context) {
		ObjectMapper mapper = ServiceLocator.getInstance().getMapperService().getMapper();
		try {
			ProducerRecord<String, String> record = new ProducerRecord<>(context.getApplication(), context.getKey(), mapper.writeValueAsString(context));
	        kafkaproducer.send(record);
		} catch (JsonProcessingException e) {
			log.error(e.getLocalizedMessage(), e);
		}
	}
}
