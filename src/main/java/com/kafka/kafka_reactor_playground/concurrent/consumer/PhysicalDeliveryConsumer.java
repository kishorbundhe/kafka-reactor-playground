package com.kafka.kafka_reactor_playground.concurrent.consumer;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafka.kafka_reactor_playground.concurrent.dto.PhysicalDelivery;

@Configuration
public class PhysicalDeliveryConsumer {
    private static final Logger logger = LoggerFactory.getLogger(PhysicalDeliveryConsumer.class);

    @Bean
    public Consumer<PhysicalDelivery> physicalConsumer() {
        return this::logRecieved;
    }

    public void logRecieved(Object payLoad) {
        logger.info(" recieved in PhysicalDeliveryConsumer : {}", payLoad);
    }
}
