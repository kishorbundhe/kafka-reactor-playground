package com.kafka.kafka_reactor_playground.errorhandling.exception;

public class InputValidException extends RuntimeException{
    public InputValidException(String message) {
        super(message);
    }
}
