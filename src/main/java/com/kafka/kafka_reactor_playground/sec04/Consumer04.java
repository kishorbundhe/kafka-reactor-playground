package com.kafka.kafka_reactor_playground.sec04;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;

import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

/* Rebalancing and creating consumers of the group*/
public class Consumer04 {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Consumer04.class);

    public static void create(int instanceNumber) {
        var config = Map.<String, Object>of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-1",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "demo-group-1-instance-" + instanceNumber,
                ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        var options = ReceiverOptions.create(config)
                .addAssignListener(i -> {
                    i.forEach(r -> log.info(" assigned {}", r.position()));
                    //i.forEach(r-> r.seek(r.position()-2));
                   //i.forEach(r -> log.info(" after seek assigned {}", r.position()));
                   i.stream().filter(r->r.topicPartition().partition()==2)
                   .findFirst()
                   .ifPresent(r->r.seek(r.position()-2));
                })
                .subscription(List.of("topic-1"));

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("Received message: {} with key = {} ", r.value(), r.key()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();

    }

}
