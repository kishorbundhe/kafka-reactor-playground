package com.kafka.kafka_reactor_playground.concurrent.processor;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Gatherers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import com.kafka.kafka_reactor_playground.concurrent.dto.DigitalDelivery;
import com.kafka.kafka_reactor_playground.concurrent.dto.Order;
import com.kafka.kafka_reactor_playground.concurrent.dto.PhysicalDelivery;

@Configuration
@ConditionalOnProperty(name = "processing-mode", havingValue = "unordered")
public class UnorderedMessageProcessing {

    private static final Logger log = LoggerFactory.getLogger(UnorderedMessageProcessing.class);
    private static final String SPRING_CLOUD_DESTINATION = "spring.cloud.stream.sendto.destination";
    private static final String DIGITAL_OUT = "digital-delivery-out";
    private static final String PHYSICAL_OUT = "physical-delivery-out";
    private static final int MAX_CONCURRENCY = 500;

    // This is used as an example for framework level concurrency
    @Bean
    public Function<List<Order>, List<Message<Object>>> deliveryProcessor(DeliveryService deliveryService) {
        // we cannot use .parallel () because underlying threads are mostly used for cpu
        // intensive operation
        // not for I/O calls
        return orders -> orders.stream()
                // .map(deliveryService::toDelivery)
                .gather(Gatherers.mapConcurrent(MAX_CONCURRENCY, deliveryService::toDelivery)) // 500 virtual threads
                 // will be created
                .map(this::toMessage).toList();
    }

    private Message<Object> toMessage(Object payload) {
        if (payload instanceof DigitalDelivery delivery) {
            log.info(" dispatching order" + delivery.orderId());
        } else {
            log.info(" dispatching order" + ((PhysicalDelivery) payload).orderId());
        }

        var destination = (payload instanceof DigitalDelivery) ? DIGITAL_OUT : PHYSICAL_OUT;
        return MessageBuilder.withPayload(payload)
                .setHeader(SPRING_CLOUD_DESTINATION, destination).build();
    }

}
