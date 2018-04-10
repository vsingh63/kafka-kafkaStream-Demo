package com.credit.docs.publishFileToTopic.service;

import java.util.Random;

public class GetResponseFromOCR {
	
	private Random random;

    public GetResponseFromOCR() {
        // ...
        random = new Random();
    }

    public boolean getRandomBoolean() {
        return random.nextBoolean();
    }

}
