package com.example.kafka.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Topic settings bound from {@code app.kafka.topics.*}.
 */
@ConfigurationProperties(prefix = "app.kafka.topics")
public record TopicProperties(String orders, int partitions) {
}
