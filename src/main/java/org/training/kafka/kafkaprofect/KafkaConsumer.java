package org.training.kafka.kafkaprofect;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(topics = "test1", groupId = "test-consumer",concurrency = "10")
    public void readMessage(String message) {
        System.out.println("Received Message : " + message);
    }

}
