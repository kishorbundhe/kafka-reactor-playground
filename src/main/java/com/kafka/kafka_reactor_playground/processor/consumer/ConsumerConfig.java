package com.kafka.kafka_reactor_playground.processor.consumer;

import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafka.kafka_reactor_playground.processor.dto.Payment;

@Configuration
public class ConsumerConfig {

    Logger logger = LoggerFactory.getLogger(ConsumerConfig.class);

    @Bean
    public Consumer<Payment> paymenConsumer(){
        return this::logRecieved;
    } 

    public void logRecieved(Object payLoad){
        logger.info(" recieved {}",payLoad );
    }
}
