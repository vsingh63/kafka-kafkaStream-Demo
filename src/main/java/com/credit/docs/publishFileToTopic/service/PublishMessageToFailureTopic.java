package com.credit.docs.publishFileToTopic.service;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class PublishMessageToFailureTopic {
	
private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	
	public void publishFailureResponse (String modifiedRecord) {
		long time = System.currentTimeMillis();
		String failureMessage = modifiedRecord.replace("FIRST FILE", "FAILURE");
		System.out.println("failure record : " + failureMessage);
		final Producer<String, String> producer = createProducer();
		try {
			final ProducerRecord<String, String> record = new ProducerRecord<>("failure-topic", String.valueOf(time),  failureMessage);
			producer.send(record);
		} finally {
			producer.flush();
			producer.close();
		}
		
	}
	
	
	private Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkafailureProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);

	}

}
