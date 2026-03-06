package com.kafka.kafka_reactor_playground.processor.dto;

public record Payment(int transactionId, int orderId, int amount) {

}
