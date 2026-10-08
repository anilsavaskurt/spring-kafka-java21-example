package com.example.kafka.domain;

public enum OrderStatus {
    /** Published to Kafka, not yet processed by the consumer. */
    PENDING,
    CONFIRMED,
    CANCELLED,
    /** Could not be processed and ended up on the dead-letter topic. */
    FAILED
}
