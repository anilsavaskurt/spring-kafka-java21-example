package com.example.kafka.domain;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the polymorphic JSON round trip with the same serializers Kafka uses, without a broker.
 */
class OrderEventJsonTest {

    private final JsonSerializer<OrderEvent> serializer = new JsonSerializer<OrderEvent>().noTypeInfo();
    private final JsonDeserializer<OrderEvent> deserializer = new JsonDeserializer<>(OrderEvent.class).ignoreTypeHeaders();

    @Test
    void orderCreatedRoundTrip() {
        OrderEvent event = new OrderEvent.OrderCreated(UUID.randomUUID(), "customer-1",
                List.of(new OrderItem("BOOK-1", 2, new BigDecimal("12.50"))), Instant.now());

        byte[] json = serializer.serialize("orders", event);

        assertThat(new String(json, StandardCharsets.UTF_8)).contains("\"type\":\"ORDER_CREATED\"");
        assertThat(deserializer.deserialize("orders", json)).isEqualTo(event);
    }

    @Test
    void orderCancelledRoundTrip() {
        OrderEvent event = new OrderEvent.OrderCancelled(UUID.randomUUID(), "changed my mind", Instant.now());

        byte[] json = serializer.serialize("orders", event);

        assertThat(deserializer.deserialize("orders", json))
                .isInstanceOf(OrderEvent.OrderCancelled.class)
                .isEqualTo(event);
    }

    @Test
    void totalSumsLineTotals() {
        var created = new OrderEvent.OrderCreated(UUID.randomUUID(), "customer-1", List.of(
                new OrderItem("A", 2, new BigDecimal("10.00")),
                new OrderItem("B", 1, new BigDecimal("5.25"))), Instant.now());

        assertThat(created.total()).isEqualByComparingTo("25.25");
    }
}
