package com.kafka.kafka_reactor_playground.processor.dto;

public record Order(int id, int customerId, int amount, ProductType productType) {

}
