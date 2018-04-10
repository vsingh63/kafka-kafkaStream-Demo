package com.credit.docs.publishFileToTopic.controller;

import java.util.Properties;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class KafkaProducerController {

	private final static String BOOTSTRAP_SERVERS = "localhost:9092";

	@RequestMapping(path = "/v1/publish/{topic}", method = RequestMethod.GET)
	public ResponseEntity publishDataToTopic(@PathVariable(value = "topic") String topic) throws Exception {
		System.out.println("message" + topic);
		runProducer(topic);
		//Dummy response
		return ResponseEntity.ok("success");

	}

	private Producer<String, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);

	}

	public void runProducer(final String topic) throws Exception {
		final Producer<String, String> producer = createProducer();
		long time = System.currentTimeMillis();
		try {

			final ProducerRecord<String, String> record = new ProducerRecord<>(topic, String.valueOf(time), "First file " + time);

			 producer.send(record, (metadata, exception) -> {
				 long elapsedTime = System.currentTimeMillis() - time;
				 if (metadata != null) {
	                    System.out.printf("sent record(key=%s value=%s) " +
	                                    "meta(partition=%d, offset=%d) time=%d\n",
	                            record.key(), record.value(), metadata.partition(),
	                            metadata.offset(), elapsedTime);
	                } else {
	                    exception.printStackTrace();
	                }
			 });
			 
		} finally {
			producer.flush();
			producer.close();
		}
		
	}
}
