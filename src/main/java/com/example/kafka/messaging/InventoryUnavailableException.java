package com.example.kafka.messaging;

/**
 * A temporary failure: worth retrying, the stock may come back.
 */
public class InventoryUnavailableException extends RuntimeException {

    public InventoryUnavailableException(String message) {
        super(message);
    }
}
