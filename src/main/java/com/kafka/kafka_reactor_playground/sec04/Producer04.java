package com.kafka.kafka_reactor_playground.sec04;

import java.time.Duration;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

public class Producer04 {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(Producer04.class);

    public static void main(String[] args) {
        Map<String, Object> config = Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        var  options = SenderOptions.<String, String>create(config).maxInFlight(10000);

        var flux = Flux.interval(Duration.ofMillis(50)).take(10000)
                .map(i -> {
                    var record = new ProducerRecord<>("topic-1", "key-" + i, "value-" + i);
                    return record;
                })
                .map(i -> SenderRecord.create(i, i.key()));

        final long now = System.currentTimeMillis();
        var sender = KafkaSender.create(options);
        sender.send(flux)
                .doOnNext(i -> log.info("Message sent: {}", i.correlationMetadata()))
                .doOnComplete(() -> {
                    log.info("All messages sent in {} ms", System.currentTimeMillis() - now);
                    sender.close();
                })
                .subscribe();

    }
}
