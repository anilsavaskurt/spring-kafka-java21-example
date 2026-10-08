package com.example.kafka.order;

import com.example.kafka.domain.OrderStatus;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Read model of an order, as returned by the REST API.
 */
public record Order(
        UUID id,
        String customerId,
        BigDecimal total,
        OrderStatus status,
        String failureReason,
        Instant updatedAt) {

    public static Order pending(UUID id, String customerId, BigDecimal total) {
        return new Order(id, customerId, total, OrderStatus.PENDING, null, Instant.now());
    }

    public Order withStatus(OrderStatus newStatus, String reason) {
        return new Order(id, customerId, total, newStatus, reason, Instant.now());
    }
}
