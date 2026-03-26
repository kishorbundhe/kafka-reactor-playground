package com.kafka.kafka_reactor_playground.errorhandling.consumer;

import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafka.kafka_reactor_playground.errorhandling.exception.InputValidException;
import com.kafka.kafka_reactor_playground.errorhandling.exception.ServiceNotAvailableException;

@Configuration
public class ConsumerConfig {

    Logger logger = LoggerFactory.getLogger(ConsumerConfig.class);

    @Bean
    public Consumer<Integer> consumer() {
        return this::handleMessage;
    }

    private void handleMessage(Integer orderId) {
        logger.info("Recieved order {}", orderId);
        if (orderId > 5) {
            simulateFailureTemporary();
        } else {
            simulateInputValidationFailure();
        }

    }

    private void simulateFailureTemporary() {
        int random = ThreadLocalRandom.current().nextInt(0, 11);
        logger.info("random {}", random);
        if (random > 8) {
            logger.info("processing succeeded");

        } else {
            logger.info("Service is not available");
            throw new ServiceNotAvailableException("Service is not available");
        }
    }

    private void simulateInputValidationFailure() throws InputValidException {
        throw new InputValidException("Input is not valid");
    }
}
