package com.kafka.kafka_reactor_playground.errorhandling.exception;

public class ServiceNotAvailableException extends RuntimeException {

    public ServiceNotAvailableException(String message) {
        super(message);
    }

}
