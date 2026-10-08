package com.example.kafka.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares topics so that {@code KafkaAdmin} creates them on startup.
 * Retry and DLT topics are created automatically by {@code @RetryableTopic}.
 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    NewTopic ordersTopic(TopicProperties topics) {
        return TopicBuilder.name(topics.orders())
                .partitions(topics.partitions())
                .replicas(1)
                .build();
    }
}
