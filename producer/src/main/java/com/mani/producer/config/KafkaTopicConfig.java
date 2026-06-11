package com.mani.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

// creating a new topic programmatically
@Configuration
public class KafkaTopicConfig {

    public static final String TOPIC_NAME = "orders-topic";

    @Bean
    public NewTopic createTopic() {
        return new NewTopic(TOPIC_NAME, 3, (short) 1);
    }
}
