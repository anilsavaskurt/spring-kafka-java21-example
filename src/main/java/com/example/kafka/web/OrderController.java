package com.example.kafka.web;

import com.example.kafka.order.Order;
import com.example.kafka.order.OrderService;
import com.example.kafka.order.OrderStore;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;

import java.util.Collection;
import java.util.UUID;

/**
 * Writes return {@code 202 Accepted}: the request is on Kafka, but processing happens later.
 * Poll {@code GET /api/orders/{id}} to see the status change.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderService orderService;
    private final OrderStore orderStore;

    public OrderController(OrderService orderService, OrderStore orderStore) {
        this.orderService = orderService;
        this.orderStore = orderStore;
    }

    @PostMapping
    public ResponseEntity<Order> create(@Valid @RequestBody CreateOrderRequest request) {
        var order = orderService.placeOrder(request.customerId(), request.items());
        var location = ServletUriComponentsBuilder.fromCurrentRequest()
                .path("/{id}")
                .buildAndExpand(order.id())
                .toUri();
        return ResponseEntity.accepted().location(location).body(order);
    }

    @GetMapping
    public Collection<Order> list() {
        return orderStore.findAll();
    }

    @GetMapping("/{id}")
    public Order get(@PathVariable UUID id) {
        return orderStore.findById(id).orElseThrow(() -> notFound(id));
    }

    @PostMapping("/{id}/cancel")
    public ResponseEntity<Order> cancel(@PathVariable UUID id, @Valid @RequestBody CancelOrderRequest request) {
        return orderService.cancelOrder(id, request.reason())
                .map(order -> ResponseEntity.accepted().body(order))
                .orElseThrow(() -> notFound(id));
    }

    private static ResponseStatusException notFound(UUID id) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, "Order " + id + " not found");
    }
}
