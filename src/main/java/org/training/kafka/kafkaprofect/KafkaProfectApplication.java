package org.training.kafka.kafkaprofect;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@SpringBootApplication
@EnableAsync
@EnableKafka
@EnableKafkaStreams
public class KafkaProfectApplication {

    @Async
    public Future<String> method(){
        return CompletableFuture.completedFuture("osman");
    }

    public static void main(String[] args) {
        SpringApplication.run(KafkaProfectApplication.class,
                              args);
    }

}
