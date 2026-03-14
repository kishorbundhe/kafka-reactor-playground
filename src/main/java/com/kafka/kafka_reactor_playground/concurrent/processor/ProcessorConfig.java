package com.kafka.kafka_reactor_playground.concurrent.processor;

import java.util.function.Function;

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
@ConditionalOnProperty(name = "processing-mode", havingValue = "normal")
public class ProcessorConfig {

    private static final Logger log = LoggerFactory.getLogger(ProcessorConfig.class);
    private static final String SPRING_CLOUD_DESTINATION = "spring.cloud.stream.sendto.destination";
    private static final String DIGITAL_OUT = "digital-delivery-out";
    private static final String PHYSICAL_OUT = "physical-delivery-out";

    // This is used as an example for framework level concurrency
    @Bean
    public Function<Order, Message<?>> deliveryProcessor(DeliveryService deliveryService) {
        return order -> this.toMessage(deliveryService.toDelivery(order));
    }

    private Message<?> toMessage(Object payload) {
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
