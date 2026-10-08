package com.example.kafka.messaging;

import com.example.kafka.config.TopicProperties;
import com.example.kafka.domain.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
public class OrderEventPublisher {

    private static final Logger log = LoggerFactory.getLogger(OrderEventPublisher.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private final TopicProperties topics;

    public OrderEventPublisher(KafkaTemplate<String, OrderEvent> kafkaTemplate, TopicProperties topics) {
        this.kafkaTemplate = kafkaTemplate;
        this.topics = topics;
    }

    /**
     * The order id is the record key, so all events of one order land on the same
     * partition and are consumed in the order they were sent.
     */
    public CompletableFuture<SendResult<String, OrderEvent>> publish(OrderEvent event) {
        return kafkaTemplate.send(topics.orders(), event.orderId().toString(), event)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        var metadata = result.getRecordMetadata();
                        log.info("Sent {} for order {} to {}-{}@{}", event.getClass().getSimpleName(),
                                event.orderId(), metadata.topic(), metadata.partition(), metadata.offset());
                    }
                });
    }
}
