package com.kafka.kafka_reactor_playground.concurrent.consumer;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafka.kafka_reactor_playground.concurrent.dto.DigitalDelivery;

@Configuration
public class DigitalDeliveryConsumer {

    private static final Logger logger = LoggerFactory.getLogger(DigitalDeliveryConsumer.class);

    @Bean
    public Consumer<DigitalDelivery> digitalConsumer() {
        return this::logRecieved;
    }

    public void logRecieved(Object payLoad) {
        logger.info(" recieved in DigitalDeliveryConsumer: {}", payLoad);
    }
}
