package org.training.kafka.kafkaprofect;

import com.training.kafka.avro.OrderEvent;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaStreamConfiguration {

    @Value("${spring.kafka.streams.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Autowired
    public void buildPipeline1(StreamsBuilder streamsBuilder) {
        KStream<String, String> orderStream = streamsBuilder.stream(
                "orders-text",
                Consumed.with(Serdes.String(),
                              Serdes.String())
        );

        // 2. Apply transformations (Filter & Map)
        KStream<String, String> processedStream = orderStream
                .filter((key, value) -> {
                    return value != null && value.contains("PREMIUM");
                })
                .mapValues(value -> value.toUpperCase());

        processedStream.to(
                "orders-high-value1",
                Produced.with(Serdes.String(),
                              Serdes.String())
        );
    }

    @Autowired
    public void buildPipeline2(StreamsBuilder streamsBuilder) {
        KStream<String, OrderEvent> orderStream = streamsBuilder.stream(
                "orders",
                Consumed.with(Serdes.String(),
                              orderEventSerde())
        );

        KStream<String, OrderEvent> processedStream = orderStream
                .filter((key, value) -> {
                    return value.getAmount() > 1000;
                });

        processedStream.to(
                "orders-high-value2",
                Produced.with(Serdes.String(),
                              orderEventSerde())
        );
    }

    @Bean
    public Serde<OrderEvent> orderEventSerde() {
        SpecificAvroSerde<OrderEvent> serde = new SpecificAvroSerde();

        Map<String, String> config = new HashMap<>();
        config.put("schema.registry.url", schemaRegistryUrl);
        config.put("specific.avro.reader", "true");

        serde.configure(config, false); // 'false' specifies this is for record values, not keys
        return serde;
    }

}

