package com.kafka.kafka_reactor_playground.acknowledge.consumer;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

import org.springframework.kafka.support.Acknowledgment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;

@Configuration
public class ConsumerConfig {

    Logger logger = LoggerFactory.getLogger(ConsumerConfig.class);

    @Bean
    public Consumer<Message<String>> consumer() {
        return this::handleMessage;
    }

    private void handleMessage(Message<String> msg) {
        logger.info("Recieved payload {}", msg.getPayload());
        // avaiable only for when ack-mode = MANUAL
        var acknowledgement = msg.getHeaders().get(KafkaHeaders.ACKNOWLEDGMENT, Acknowledgment.class);

        if (Objects.isNull(acknowledgement)) {
            throw new IllegalStateException("ack is required in manual mode");
        }

        switch (msg.getPayload()) {
            case "4", "5", "6":
                simulateNoAck();
                break;
            case "7":
                simulateFailureTemporary(acknowledgement);
            default:
                acknowledgement.acknowledge();
        }
    }

    private void simulateFailureTemporary(Acknowledgment acknowledgement) {
        int random = ThreadLocalRandom.current().nextInt(0, 11);
        logger.info("random {}", random);
        if (random > 8) {
            logger.info("processing succeeded");
            acknowledgement.acknowledge();
        } else {
            logger.info("temporary failure, retrying after 5 minutes ");
            acknowledgement.nack(Duration.ofSeconds(5));
        }
    }

    private void simulateNoAck() {
        logger.info("message processed but not acknowledged");
    }
}
