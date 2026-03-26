package com.kafka.kafka_reactor_playground.playandpause.producer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Logger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ProducerConfig {
    Logger logger = Logger.getLogger(ProducerConfig.class.getName());

    @Bean
    public Supplier<String> producer() {

        var counter = new AtomicInteger(0);
        logger.info("produced msg: " + counter);
        return () -> {
            logger.info("produced msg: " + counter);
            return "msg " + counter.incrementAndGet();
        };
    }
}
