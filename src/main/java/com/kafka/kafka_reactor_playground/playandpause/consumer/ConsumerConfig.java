package com.kafka.kafka_reactor_playground.playandpause.consumer;

import java.util.function.Consumer;
import java.util.logging.Logger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ConsumerConfig {

    Logger logger = Logger.getLogger(ConsumerConfig.class.getName());

    @Bean
    public Consumer<String> consumer() {
        return msg -> {
           
            logger.info("Recieved msg : " + msg);
        };
    }

}
