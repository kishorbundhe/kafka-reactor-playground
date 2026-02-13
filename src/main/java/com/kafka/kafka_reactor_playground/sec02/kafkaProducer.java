package com.kafka.kafka_reactor_playground.sec02;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

public class kafkaProducer {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(kafkaProducer.class);

    public static void main(String[] args) {
        Map<String, Object> config = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        SenderOptions<String, String> options = SenderOptions.create(config);

        var flux = Flux.range(0, 100)
                .map(i -> new ProducerRecord<>("topic-1", "key-" + i, "value-" + i))
                .map(i -> SenderRecord.create(i, i.key()));

        var sender = KafkaSender
                .create(options);
        sender
                .send(flux)
                .doOnNext(i -> log.info("Message sent: {}", i.correlationMetadata()))
                .doOnComplete(sender::close)
                .subscribe();

    }
}
