package com.example.kafka.messaging;

import com.example.kafka.domain.OrderEvent;
import com.example.kafka.domain.OrderEvent.OrderCancelled;
import com.example.kafka.domain.OrderEvent.OrderCreated;
import com.example.kafka.domain.OrderItem;
import com.example.kafka.domain.OrderStatus;
import com.example.kafka.order.OrderStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.EnumSet;

/**
 * Processes order events.
 * <p>
 * A failing message is not retried in place, which would block the partition. Instead
 * {@code @RetryableTopic} forwards it to {@code orders-retry-0}, {@code orders-retry-1}, ...
 * with growing delays, and finally to {@code orders-dlt}.
 */
@Component
public class OrderEventListener {

    /** Any item with this SKU simulates a temporary inventory outage. */
    public static final String OUT_OF_STOCK_SKU = "OUT-OF-STOCK";

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final OrderStore store;

    public OrderEventListener(OrderStore store) {
        this.store = store;
    }

    @RetryableTopic(
            attempts = "${app.kafka.retry.attempts}",
            backoff = @Backoff(
                    delayExpression = "${app.kafka.retry.delay-ms}",
                    multiplierExpression = "${app.kafka.retry.multiplier}"),
            numPartitions = "${app.kafka.topics.partitions}",
            replicationFactor = "1",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            dltStrategy = DltStrategy.FAIL_ON_ERROR,
            exclude = InvalidOrderException.class)
    @KafkaListener(topics = "${app.kafka.topics.orders}")
    public void onOrderEvent(OrderEvent event, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Received {} for order {} from {}", event.getClass().getSimpleName(), event.orderId(), topic);

        // OrderEvent is sealed: this switch stops compiling if a new event type is not handled.
        switch (event) {
            case OrderCreated created -> confirm(created);
            case OrderCancelled cancelled -> cancel(cancelled);
        }
    }

    @DltHandler
    public void onDeadLetter(OrderEvent event,
                             @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                             @Header(name = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String error) {
        log.warn("Order {} ended up in {}: {}", event.orderId(), topic, error);
        store.forceStatus(event.orderId(), OrderStatus.FAILED, error);
    }

    private void confirm(OrderCreated created) {
        if (created.items().isEmpty()) {
            throw new InvalidOrderException("Order " + created.orderId() + " has no items");
        }
        // Record pattern: destructures the item and binds its sku in one step.
        boolean outOfStock = created.items().stream()
                .anyMatch(item -> item instanceof OrderItem(var sku, var quantity, var unitPrice)
                        && OUT_OF_STOCK_SKU.equals(sku));
        if (outOfStock) {
            throw new InventoryUnavailableException("Item " + OUT_OF_STOCK_SKU + " is not in stock");
        }

        if (store.transition(created.orderId(), EnumSet.of(OrderStatus.PENDING), OrderStatus.CONFIRMED, null)) {
            log.info("Order {} confirmed, total {}", created.orderId(), created.total());
        }
    }

    private void cancel(OrderCancelled cancelled) {
        var cancellable = EnumSet.of(OrderStatus.PENDING, OrderStatus.CONFIRMED);
        if (store.transition(cancelled.orderId(), cancellable, OrderStatus.CANCELLED, cancelled.reason())) {
            log.info("Order {} cancelled: {}", cancelled.orderId(), cancelled.reason());
        }
    }
}
