package com.kafka.kafka_reactor_playground.parallelprocessing.flatmap;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

/* Rebalancing and creating consumers of the group*/
public class ConsumerFlatMap {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ConsumerGroupByFlatMap.class);

    public static void main(String[] args) {
        create(1);
    }

    public static void create(int instanceNumber) {
        var config = Map.<String, Object>of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-1",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "demo-group-1-instance-" + instanceNumber,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3,
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        var options = ReceiverOptions.create(config)
                .subscription(List.of("topic-1"));

        KafkaReceiver.create(options)
                .receiveAutoAck()
                .log()
                .flatMap(ConsumerFlatMap::batchProcess, 10) // can subscribe to 10 items in parallel, default is 256
                .subscribe();

    }

    private static Mono<Void> batchProcess(Flux<ConsumerRecord<Object, Object>> records) {
        // if we want to use another thread to process the batch, we can use
        // publishOn(Schedulers.boundedElastic()) before doOnNext
        return records.publishOn(Schedulers.boundedElastic())
                .doFirst(() -> log.info("------------------------"))
                .doOnNext(r -> log.info("Processing message: {} with key = {} ", r.value(), r.key()))
                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();

    }
}
