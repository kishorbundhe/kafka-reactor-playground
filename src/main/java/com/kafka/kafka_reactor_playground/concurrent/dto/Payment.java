package com.kafka.kafka_reactor_playground.concurrent.dto;

public record Payment(int transactionId, int orderId, int amount) {

}
