package com.example.kafka.messaging;

/**
 * A permanent failure: retrying will never help, so the message goes straight to the DLT.
 */
public class InvalidOrderException extends RuntimeException {

    public InvalidOrderException(String message) {
        super(message);
    }
}
