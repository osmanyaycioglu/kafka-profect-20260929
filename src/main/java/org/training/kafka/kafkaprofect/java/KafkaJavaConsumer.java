package org.training.kafka.kafkaprofect.java;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class KafkaJavaConsumer {

    private static BlockingQueue<ConsumerRecord<Integer, String>> records = new ArrayBlockingQueue<>(10_000);


    public static class KafkaWorkerThread extends Thread {
        private final KafkaConsumer<Integer, String> kafkaConsumerLoc;

        public KafkaWorkerThread(final KafkaConsumer<Integer, String> kafkaConsumerLocParam) {
            kafkaConsumerLoc = kafkaConsumerLocParam;
        }

        @Override
        public void run() {
            while (true) {
                try {
                    ConsumerRecord<Integer, String> takeLoc = records.take();
                    System.out.println("Received message : " + takeLoc + " Thread : " + Thread.currentThread()
                                                                                              .getName());

//                    Map<TopicPartition, OffsetAndMetadata> mapLoc = new HashMap<>();
//                    mapLoc.put(new TopicPartition(takeLoc.topic(),
//                                                  takeLoc.partition()),
//                               new OffsetAndMetadata(takeLoc.offset()));
//                    kafkaConsumerLoc.commitSync(mapLoc);
                } catch (Exception exp) {
                    exp.printStackTrace();
                }

            }
        }
    }


    public static void main(String[] args) {
        Properties propertiesLoc = new Properties();
        propertiesLoc.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                          "127.0.0.1:9092,127.0.0.1:9093");
        propertiesLoc.put(ConsumerConfig.GROUP_ID_CONFIG,
                          "java-group-1");
        propertiesLoc.put(ConsumerConfig.CLIENT_ID_CONFIG,
                          "client-1");
        propertiesLoc.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG,
                          "java-listener-1");
        propertiesLoc.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                          IntegerDeserializer.class.getName());
        propertiesLoc.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                          StringDeserializer.class.getName());
        propertiesLoc.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                          "earliest");

        propertiesLoc.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                          "false");

        try (KafkaConsumer<Integer, String> kafkaConsumerLoc = new KafkaConsumer<>(propertiesLoc)) {
            kafkaConsumerLoc.subscribe(Arrays.asList("first-topic"));
            for (int i = 0; i < 5; i++) {
                KafkaWorkerThread threadLoc = new KafkaWorkerThread(kafkaConsumerLoc);
                threadLoc.setName("Kafka-TH-" + i);
                threadLoc.start();
            }
            while (true) {
                ConsumerRecords<Integer, String> pollLoc = kafkaConsumerLoc.poll(Duration.ofMillis(1_000));
                for (ConsumerRecord<Integer, String> recordLoc : pollLoc) {
                    records.add(recordLoc);
                }
                kafkaConsumerLoc.commitAsync();
                kafkaConsumerLoc.commitSync();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException eParam) {
                }
            }
        }

    }
}
