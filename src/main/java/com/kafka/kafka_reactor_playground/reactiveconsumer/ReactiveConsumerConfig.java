package com.kafka.kafka_reactor_playground.reactiveconsumer;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.logging.Logger;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Configuration
public class ReactiveConsumerConfig {
    Logger logger = Logger.getLogger(ReactiveConsumerConfig.class.getName());

    // @Bean
    // public Consumer<Flux<String>> reactiveConsumer() {
    //     return flux -> flux
    //             .doOnNext(msg -> logger.info("Received message from reactive consumer: " + msg))
    //             .subscribe();
    // }

    @Bean
    public Function<Flux<String>, Mono<Void>> reactiveConsumer() {
        return flux -> flux
                .doOnNext(msg -> logger.info("Received message from reactive consumer: " + msg))
                .then();
    }

}
