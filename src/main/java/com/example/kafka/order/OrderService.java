package com.example.kafka.order;

import com.example.kafka.domain.OrderEvent;
import com.example.kafka.domain.OrderItem;
import com.example.kafka.domain.OrderStatus;
import com.example.kafka.messaging.OrderEventPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderStore store;
    private final OrderEventPublisher publisher;

    public OrderService(OrderStore store, OrderEventPublisher publisher) {
        this.store = store;
        this.publisher = publisher;
    }

    /**
     * Stores the order as {@code PENDING} and publishes {@code OrderCreated}.
     * The consumer confirms it asynchronously.
     */
    public Order placeOrder(String customerId, List<OrderItem> items) {
        var event = new OrderEvent.OrderCreated(UUID.randomUUID(), customerId, List.copyOf(items), Instant.now());
        var order = store.save(Order.pending(event.orderId(), customerId, event.total()));
        publish(event);
        return order;
    }

    public Optional<Order> cancelOrder(UUID orderId, String reason) {
        return store.findById(orderId).map(order -> {
            publish(new OrderEvent.OrderCancelled(orderId, reason, Instant.now()));
            return order;
        });
    }

    private void publish(OrderEvent event) {
        publisher.publish(event).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("Could not publish {} for order {}", event.getClass().getSimpleName(), event.orderId(), ex);
                store.forceStatus(event.orderId(), OrderStatus.FAILED, "Publish failed: " + ex.getMessage());
            }
        });
    }
}
