package com.kafka.kafka_reactor_playground.concurrent.dto;

public record Order(int id, int customerId, int amount, ProductType productType) {

}
