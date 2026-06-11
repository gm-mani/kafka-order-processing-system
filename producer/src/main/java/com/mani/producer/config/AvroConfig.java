package com.mani.producer.config;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration
public class AvroConfig {

    @Value("${spring.kafka.producer.properties.schema.registry.url}")
    private String schemaRegistryUrl;

    @Bean
    public KafkaAvroSerializer avroSerializer() {
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(
                Map.of("schema.registry.url", schemaRegistryUrl),
                false  // false = this is for values, not keys
        );
        return serializer;
    }
}