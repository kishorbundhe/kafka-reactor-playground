package com.kafka.kafka_reactor_playground.processor.process;

import java.util.function.Function;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.kafka.kafka_reactor_playground.processor.dto.Order;
import com.kafka.kafka_reactor_playground.processor.dto.Payment;
import org.springframework.messaging.Message;

@Configuration
public class ProcessorConfig {

    @Bean
    public Function<Message<Order>,Payment> paymentProcessor(){
        return  msgOrder -> new Payment(msgOrder.getPayload().id(), msgOrder.getPayload().id(), msgOrder.getPayload().amount());
    }

}
