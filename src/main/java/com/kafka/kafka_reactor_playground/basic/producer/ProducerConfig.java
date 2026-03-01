package com.kafka.kafka_reactor_playground.basic.producer;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Logger;

import static java.lang.Math.random;

@Configuration
public class ProducerConfig {
    Logger logger = Logger.getLogger(ProducerConfig.class.getName());

    @Bean
    public Supplier<Message<String>> producer() {

        var counter = new AtomicInteger(0);
        logger.info("produced msg: " + counter);
        return () -> {
            logger.info("produced msg: " + counter);
            return produceMessage(counter.incrementAndGet());
        };
    }

    private Message<String> produceMessage(Integer integer) {
        return MessageBuilder.withPayload("msg : " + integer)
                .setHeader(KafkaHeaders.KEY, "key-" + integer)
                .setHeader("trace-id", random()).build();
    }
}
