package com.credit.docs.publishFileToTopic;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.credit.docs.publishFileToTopic.service.StreamAPIService;
import com.credit.docs.publishFileToTopic.service.SubscribeToStreamedTopic;

@SpringBootApplication
public class UploadAndValidateDocApplication {

	public static void main(String[] args) {
		SpringApplication.run(UploadAndValidateDocApplication.class, args);
		StreamAPIService streamService = new StreamAPIService();
		streamService.processStreamData();
		SubscribeToStreamedTopic subscribeToTopic = new SubscribeToStreamedTopic();
		subscribeToTopic.SubscribeToStreamedTopics();
	}
}
