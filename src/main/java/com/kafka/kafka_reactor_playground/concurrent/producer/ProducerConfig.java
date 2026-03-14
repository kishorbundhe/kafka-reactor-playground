package com.kafka.kafka_reactor_playground.concurrent.producer;



import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

import com.kafka.kafka_reactor_playground.processor.dto.Order;
import com.kafka.kafka_reactor_playground.processor.dto.ProductType;

@Configuration
public class ProducerConfig {
    private static final Logger logger = LoggerFactory.getLogger(ProducerConfig.class);

    @Bean
    public Supplier<Message<Order>> producer() {

        var counter = new AtomicInteger(0);
        return () -> {
            var id = counter.incrementAndGet();
            int customerId = new Random().nextInt(1, 10000);
            var amount = id;
            ProductType productType = id%2==0 ? ProductType.PHYSICAL:ProductType.DIGITAL;
            Order order = new Order(id, customerId, amount, productType);
            logger.info("produced {}",order);
            return MessageBuilder.withPayload(order).setHeader(KafkaHeaders.KEY, customerId).build();
        };
    }

}
