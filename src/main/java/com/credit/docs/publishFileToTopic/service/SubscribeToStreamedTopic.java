package com.credit.docs.publishFileToTopic.service;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ser.std.StringSerializer;

public class SubscribeToStreamedTopic {
	public static final Logger LOGGER = LoggerFactory.getLogger(SubscribeToStreamedTopic.class);
	private static final long MAX_VALUE = 100;

	public void SubscribeToStreamedTopics() {
		Consumer<String, String> consumer = new KafkaConsumer<>(getConsumerConfig());
		PublishMessageToSuccessTopic success;
		PublishMessageToFailureTopic failure;
		try {
			consumer.subscribe(Arrays.asList("streamed-out-topic"));
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(MAX_VALUE);
				for (ConsumerRecord<String, String> record : records) {
					Map<String, Object> data = new HashMap<>();
					data.put("partition", record.partition());
					data.put("offset", record.offset());
					data.put("value", record.value());
					System.out.println("record" + ": " + data);
					if (GetResponseFromOCR()) {
						success = new PublishMessageToSuccessTopic();
						success.publishSuccessResponse(record.value());
					} else {
						failure = new PublishMessageToFailureTopic();
						failure.publishFailureResponse(record.value());
					}
				}
				consumer.commitAsync();
			}
		} catch (WakeupException e) {
			LOGGER.info("Kafka Consumer Wake Up Exception: ", e);
		} catch (Exception e) {
			LOGGER.info("Exception in Kafka: ", e);
		} finally {
			consumer.close();
		}

	}

	private Properties getConsumerConfig() {
		Properties props = new Properties();
		props.put(ConsumerConfig.CLIENT_ID_CONFIG, "kafka-strem-topic-subscriber");
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "strem-topic-subscriber");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		return props;

	}
	
	private Boolean GetResponseFromOCR () {
		return Math.random() < 0.5;
	}

}
