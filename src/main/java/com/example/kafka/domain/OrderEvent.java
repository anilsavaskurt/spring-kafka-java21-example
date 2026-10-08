package com.example.kafka.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

/**
 * Every message on the orders topic is an {@code OrderEvent}.
 * <p>
 * The interface is {@code sealed}, so a {@code switch} over it is checked for
 * exhaustiveness by the compiler. Jackson writes a {@code "type"} property into
 * the JSON so the consumer can rebuild the right record.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = OrderEvent.OrderCreated.class, name = "ORDER_CREATED"),
        @JsonSubTypes.Type(value = OrderEvent.OrderCancelled.class, name = "ORDER_CANCELLED")
})
public sealed interface OrderEvent {

    UUID orderId();

    Instant occurredAt();

    record OrderCreated(UUID orderId, String customerId, List<OrderItem> items, Instant occurredAt)
            implements OrderEvent {

        public BigDecimal total() {
            return items.stream()
                    .map(OrderItem::lineTotal)
                    .reduce(BigDecimal.ZERO, BigDecimal::add);
        }
    }

    record OrderCancelled(UUID orderId, String reason, Instant occurredAt) implements OrderEvent {
    }
}
