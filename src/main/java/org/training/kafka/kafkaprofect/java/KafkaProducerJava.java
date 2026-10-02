package org.training.kafka.kafkaprofect.java;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerJava {
    public static class KafkaSendCallback implements Callback {

        @Override
        public void onCompletion(final RecordMetadata metadata,
                                 final Exception exception) {
            if (exception == null) {
                System.out.println("Sent : " + metadata);
            } else {
                System.err.println("Error on send : " + metadata);
            }
        }
    }

    public static void main(String[] args) {
        System.out.println("Producer Start");
        Properties propertiesLoc = new Properties();
        propertiesLoc.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                          "127.0.0.1:29092,127.0.0.1:39092");
        propertiesLoc.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                          IntegerSerializer.class.getName());
        propertiesLoc.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                          StringSerializer.class.getName());
        ArrayList<Future<RecordMetadata>> futuresLoc = new ArrayList<>();
        try (KafkaProducer<Integer, String> producerLoc = new KafkaProducer<>(propertiesLoc)) {
            for (int i = 0; i < 1_000; i++) {
                Future<RecordMetadata> sendLoc = producerLoc.send(new ProducerRecord<>("java-test-1",
                                                                                       "osman" + i));
//                Future<RecordMetadata> sendLoc = producerLoc.send(new ProducerRecord<>("java-test-1",
//                                                                                       "osman" + i), new KafkaSendCallback());
                futuresLoc.add(sendLoc);
            }
        }
        for (Future<RecordMetadata> futureLoc : futuresLoc) {
            try {
                RecordMetadata recordMetadataLoc = futureLoc.get();
                System.out.println("Sent : " + recordMetadataLoc);
            } catch (Exception eParam) {
                eParam.printStackTrace();
            }
        }
        System.out.println("Producer finished");
    }
}
