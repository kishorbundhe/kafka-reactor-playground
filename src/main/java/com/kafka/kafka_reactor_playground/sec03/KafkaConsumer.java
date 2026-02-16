package com.kafka.kafka_reactor_playground.sec03;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
/* Rebalancing and creating consumers of the group*/
public class KafkaConsumer {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(KafkaConsumer.class);

    public static void create(int instanceNumber) {
        var config = Map.<String, Object>of(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-1",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer",
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer", 
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "demo-group-1-instance-"+instanceNumber
                );

        var options = ReceiverOptions.create(config)
        .subscription(List.of("topic-1"));

        KafkaReceiver.create(options)
                .receive()
                .doOnNext(r -> log.info("Received message: {} with key = {} ", r.value(), r.key()))
                .doOnNext(r->r.receiverOffset().acknowledge())
                .subscribe();

    }

}
