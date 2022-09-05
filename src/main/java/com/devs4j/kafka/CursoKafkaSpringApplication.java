package com.devs4j.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.concurrent.TimeUnit;

@SpringBootApplication
public class CursoKafkaSpringApplication implements CommandLineRunner {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	private static final Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);

	@KafkaListener(topics = "devs4j-topic",
				   containerFactory = "listenerContainerFactory",
				   groupId = "devs4j-group",
				   properties = {"max.poll.interval.ms:4000", "max.poll.records:10"})
	public void listen(List<String> messages) {
		log.info("Start reading messages");
		messages.forEach(message -> log.info("Message received: {}", message));
		log.info("Batch completed");
	}

	public static void main(String[] args) {
		SpringApplication.run(CursoKafkaSpringApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
		for (int i = 0; i < 100; i++) {
			kafkaTemplate.send("devs4j-topic", String.format("Sample message %d", i));
		}
	}

}
