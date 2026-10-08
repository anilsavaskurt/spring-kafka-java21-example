package com.example.kafka.order;

import com.example.kafka.domain.OrderStatus;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

@Component
public class OrderStore {

    private final OrderRepository repository;

    public OrderStore(OrderRepository repository) {
        this.repository = repository;
    }

    public Order save(Order order) {
        return repository.save(OrderEntity.fromOrder(order)).toOrder();
    }

    public Optional<Order> findById(UUID id) {
        return repository.findById(id).map(OrderEntity::toOrder);
    }

    public Collection<Order> findAll() {
        return repository.findAll().stream().map(OrderEntity::toOrder).toList();
    }

    public boolean transition(UUID id, Set<OrderStatus> allowedFrom, OrderStatus newStatus, String reason) {
        var entity = repository.findById(id);
        if (entity.isEmpty()) {
            return false;
        }

        var current = entity.get();
        if (!allowedFrom.contains(current.getStatus())) {
            return false;
        }

        current.setStatus(newStatus);
        current.setFailureReason(reason);
        current.setUpdatedAt(Instant.now());
        repository.save(current);
        return true;
    }

    public void forceStatus(UUID id, OrderStatus newStatus, String reason) {
        repository.findById(id).ifPresent(entity -> {
            entity.setStatus(newStatus);
            entity.setFailureReason(reason);
            entity.setUpdatedAt(Instant.now());
            repository.save(entity);
        });
    }
}
