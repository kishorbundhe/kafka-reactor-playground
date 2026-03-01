package com.kafka.kafka_reactor_playground.basic.consumer;

import java.util.function.Consumer;
import java.util.logging.Logger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;

@Configuration
public class ConsumerConfig {

    Logger logger = Logger.getLogger(ConsumerConfig.class.getName());

    @Bean
    public Consumer<Message<String>> consumer1() {
        return msg -> {
            logger.info("msgHeaders:  " + msg.getHeaders()); // key is KafkaHeaders.RECIEVEDKEY
            logger.info("msgPayload: " + msg.getPayload());
        };
    }

    @Bean
    public Consumer<Message<String>> consumer2() {
        return msg -> {
            logger.info("msgPayload: " + msg.getPayload());
        };
    }

}
