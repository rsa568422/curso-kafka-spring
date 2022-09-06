package com.devs4j.kafka;

import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.List;

@SpringBootApplication
public class CursoKafkaSpringApplication {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private MeterRegistry meterRegistry;

	private static final Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);

	@KafkaListener(id = "devs4jId",
				   autoStartup = "true",
				   topics = "devs4j-topic",
				   containerFactory = "listenerContainerFactory",
				   groupId = "devs4j-group",
				   properties = {"max.poll.interval.ms:4000", "max.poll.records:50"})
	public void listen(List<ConsumerRecord<String, String>> messages) {
		log.info("Messages received {}", messages.size());
	}

	public static void main(String[] args) {
		SpringApplication.run(CursoKafkaSpringApplication.class, args);
	}

	@Scheduled(fixedDelay = 2000, initialDelay = 100)
	public void sendKafkaMessages() {
		for (int i = 0; i < 200; i++) {
			kafkaTemplate.send("devs4j-topic", String.format("Sample message %d", i));
		}
	}

	@Scheduled(fixedDelay = 2000, initialDelay = 500)
	public void printMetrics() {
		List<Meter> metrics = meterRegistry.getMeters();

		metrics.forEach(metric -> log.info("Meter = {}", metric.getId().getName()));

		double count = meterRegistry.get("kafka.producer.record.send.total").functionCounter().count();
		log.info("Count {}", count);
	}

}
