package io.linkedlogics.kafka.service;

import java.time.Duration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.linkedlogics.LinkedLogics;
import io.linkedlogics.context.Context;
import io.linkedlogics.service.ConsumerService;
import io.linkedlogics.service.ServiceLocator;
import io.linkedlogics.service.task.ProcessorTask;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KafkaConsumerService implements ConsumerService, Runnable {
	private Thread consumer;
	private boolean isRunning;
	private Consumer<String, String> kafkaConsumer;
	
	public KafkaConsumerService() {
		kafkaConsumer = new KafkaConnectionService().getConsumer(LinkedLogics.getApplicationName());
	}
	
	@Override
	public void start() {
		consumer = new Thread(this);
		consumer.start();
	}

	@Override
	public void stop() {
		isRunning = false;
		if (consumer != null) {
			consumer.interrupt();
		}
	}

	@Override
	public void run() {
		isRunning = true;
		
		while (isRunning) {
			try {
				ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));
				
				for (ConsumerRecord<String, String> record : records) {
					ObjectMapper mapper = ServiceLocator.getInstance().getMapperService().getMapper();
					try {
						consume(mapper.readValue(record.value(), Context.class));
						kafkaConsumer.commitSync();
					} catch (Exception e) {
						log.error(e.getLocalizedMessage(), e);
					}
				}
			} catch (Exception e) {
				log.error(e.getLocalizedMessage(), e);
			}
		}
	}

	@Override
	public void consume(Context context) {
		ServiceLocator.getInstance().getProcessorService().process(new ProcessorTask(context));
	}
}
