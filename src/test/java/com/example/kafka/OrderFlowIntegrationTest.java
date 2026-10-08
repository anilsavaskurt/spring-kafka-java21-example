package com.example.kafka;

import com.example.kafka.domain.OrderItem;
import com.example.kafka.domain.OrderStatus;
import com.example.kafka.messaging.OrderEventListener;
import com.example.kafka.order.OrderService;
import com.example.kafka.order.OrderStore;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runs the whole flow against a real Kafka broker and PostgreSQL in Docker.
 */
@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
class OrderFlowIntegrationTest {

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Autowired
    OrderService orderService;

    @Autowired
    OrderStore orderStore;

    @Test
    void createdOrderIsConfirmed() {
        var order = orderService.placeOrder("customer-1", List.of(item("BOOK-1")));

        assertThat(order.status()).isEqualTo(OrderStatus.PENDING);
        awaitStatus(order.id(), OrderStatus.CONFIRMED);
    }

    @Test
    void cancelledOrderIsCancelled() {
        var order = orderService.placeOrder("customer-2", List.of(item("BOOK-2")));
        awaitStatus(order.id(), OrderStatus.CONFIRMED);

        orderService.cancelOrder(order.id(), "changed my mind");

        awaitStatus(order.id(), OrderStatus.CANCELLED);
        assertThat(orderStore.findById(order.id()).orElseThrow().failureReason()).isEqualTo("changed my mind");
    }

    @Test
    void orderThatKeepsFailingEndsUpInDeadLetterTopic() {
        var order = orderService.placeOrder("customer-3", List.of(item(OrderEventListener.OUT_OF_STOCK_SKU)));

        awaitStatus(order.id(), OrderStatus.FAILED);
        assertThat(orderStore.findById(order.id()).orElseThrow().failureReason()).contains("not in stock");
    }

    private void awaitStatus(UUID orderId, OrderStatus expected) {
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(orderStore.findById(orderId).orElseThrow().status()).isEqualTo(expected));
    }

    private static OrderItem item(String sku) {
        return new OrderItem(sku, 1, new BigDecimal("9.99"));
    }
}
