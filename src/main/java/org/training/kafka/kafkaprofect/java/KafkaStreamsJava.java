package org.training.kafka.kafkaprofect.java;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;


public class KafkaStreamsJava {
    public static void main(String[] args) {
        Properties propertiesLoc = new Properties();
        propertiesLoc.put(StreamsConfig.APPLICATION_ID_CONFIG,
                          "app-1");
        propertiesLoc.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                          "127.0.0.1:29092,127.0.0.1:39092");
        propertiesLoc.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                          Serdes.Integer()
                                .getClass());
        propertiesLoc.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                          Serdes.String()
                                .getClass());
        propertiesLoc.put(StreamsConfig.CLIENT_ID_CONFIG,
                          "client2");
        propertiesLoc.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                          "earliest");

        StreamsBuilder           streamsBuilderLoc = new StreamsBuilder();
        KStream<Integer, String> streamLoc         = streamsBuilderLoc.stream("java-test-1");
        streamLoc.peek(((k, v) -> System.out.println("key : " + k + " value : " + v)))
                 .mapValues(v -> v + " değiştirdim")
                 .to("to-test-topic",
                     Produced.with(Serdes.Integer(),
                                   Serdes.String()));

        streamLoc.foreach(((k, v) -> System.out.println("Another stream key : " + k + " value : " + v)));

        Topology topologyLoc = streamsBuilderLoc.build();
        KafkaStreams kafkaStreamsLoc = new KafkaStreams(topologyLoc,
                                                        propertiesLoc);
        System.out.println("starting stream");
        kafkaStreamsLoc.start();

        Runtime.getRuntime()
               .addShutdownHook(new Thread(() -> {
                   kafkaStreamsLoc.close();
               }));

    }

}
