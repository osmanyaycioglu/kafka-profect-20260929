package org.training.kafka.kafkaprofect;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer {

    @KafkaListener(id = "xyz",
            clientIdPrefix = "test-client",
            topics = "test1",
            groupId = "test-consumer",
            concurrency = "10",
            ackMode = "MANUAL")
    public void readMessage(String message,
                            Acknowledgment acknowledgmentParam) {
        System.out.println("Received Message : " + message);
        acknowledgmentParam.acknowledge();
    }

}
