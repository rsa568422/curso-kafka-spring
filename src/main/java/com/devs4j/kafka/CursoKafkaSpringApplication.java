package com.devs4j.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

@SpringBootApplication
public class CursoKafkaSpringApplication implements CommandLineRunner {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private KafkaListenerEndpointRegistry registry;

	private static final Logger log = LoggerFactory.getLogger(CursoKafkaSpringApplication.class);

	@KafkaListener(id = "devs4jId",
				   autoStartup = "false",
				   topics = "devs4j-topic",
				   containerFactory = "listenerContainerFactory",
				   groupId = "devs4j-group",
				   properties = {"max.poll.interval.ms:4000", "max.poll.records:10"})
	public void listen(List<ConsumerRecord<String, String>> messages) {
		log.info("Start reading messages");

		messages.forEach(message ->
				log.info("Partition = {}, Offset = {}, Key = {}, Value = {}",
						message.partition(),
						message.offset(),
						message.key(),
						message.value()));

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

		log.info("Waiting to start");
		Thread.sleep(5000);
		log.info("Starting");
		registry.getListenerContainer("devs4jId").start();
		log.info("Waiting to stop");
		Thread.sleep(5000);
		registry.getListenerContainer("devs4jId").stop();
	}

}
