package com.mani.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// creating a new topic programmatically
@Configuration
public class KafkaTopicConfig {

    public static final String TOPIC_NAME = "orders-topic";

    @Bean
    public NewTopic createTopic(@Value("${kafka.topics.orders.partitions:3}") int partitions) {
        return new NewTopic(TOPIC_NAME, partitions, (short) 1);
    }
}
