package com.kafka.kafka_reactor_playground.processor.producer;

import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.logging.Logger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;

import com.kafka.kafka_reactor_playground.processor.dto.Order;
import com.kafka.kafka_reactor_playground.processor.dto.ProductType;

@Configuration
public class ProducerConfig {
    Logger logger = Logger.getLogger(ProducerConfig.class.getName());

    @Bean
    public Supplier<Message<Order>> producer() {

        var counter = new AtomicInteger(0);
        return () -> {
            var id = counter.incrementAndGet();
            int customerId = new Random().nextInt(1, 10000);
            var amount = id;
            ProductType productType = id%2==0 ? ProductType.PHYSICAL:ProductType.DIGITAL;
            Order order = new Order(id, customerId, amount, productType);
            logger.info("Produced msg"+ order);
            return MessageBuilder.withPayload(order).build();
        };
    }

}
