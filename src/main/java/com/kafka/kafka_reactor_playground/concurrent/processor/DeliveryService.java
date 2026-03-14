package com.kafka.kafka_reactor_playground.concurrent.processor;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.kafka.kafka_reactor_playground.concurrent.dto.DigitalDelivery;
import com.kafka.kafka_reactor_playground.concurrent.dto.Order;
import com.kafka.kafka_reactor_playground.concurrent.dto.PhysicalDelivery;

@Service
public class DeliveryService {
    private static final Logger log = LoggerFactory.getLogger(ProcessorConfig.class);

    public Object toDelivery(Order order) {

        log.info("recieved order "+ order.id());
        this.simulateNetworkCall();
        return switch (order.productType()) {
            case DIGITAL -> this.digitalDelivery(order);
            case PHYSICAL -> this.physicalDelivery(order);

        };

    }

    private DigitalDelivery digitalDelivery(Order order) {
        return new DigitalDelivery(order.id(), "user. %d @gmail.com".formatted(order.customerId()));
    }

    private PhysicalDelivery physicalDelivery(Order order) {
        return new PhysicalDelivery(order.id(), " %d street".formatted(order.customerId()),
                "state %d".formatted(order.customerId()));
    }

    private void simulateNetworkCall() {
        try {
            Thread.sleep(Duration.ofMillis(200));
        } catch (InterruptedException exception) {
            throw new RuntimeException(exception);
        }
    }

}
