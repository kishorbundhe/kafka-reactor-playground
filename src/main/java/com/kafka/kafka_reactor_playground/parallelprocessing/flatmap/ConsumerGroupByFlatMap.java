package com.kafka.kafka_reactor_playground.parallelprocessing.flatmap;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;

import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

/* Rebalancing and creating consumers of the group*/
public class ConsumerGroupByFlatMap {
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
                .receive()
                .groupBy(r -> Integer.parseInt(r.key().toString()) % 5)
                .flatMap(ConsumerGroupByFlatMap::batchProcess) // can subscribe to 10 items in parallel, default is //
                                                               // 256
                .subscribe();

    }

    private static Mono<Void> batchProcess(GroupedFlux<Integer, ReceiverRecord<Object, Object>> records) {
        // if we want to use another thread to process the batch, we can use
        // publishOn(Schedulers.boundedElastic()) before doOnNext
        return records.publishOn(Schedulers.boundedElastic())
                .doFirst(() -> log.info("------------------------"))
                .doOnNext(r -> log.info("Processing message with group {} : {} with key = {}  ", records.key(), r.value(), r.key()))
                .doOnNext(r-> r.receiverOffset().acknowledge())
                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();

    }
}
