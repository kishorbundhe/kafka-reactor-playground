package com.kafka.kafka_reactor_playground.sec02;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
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

        var  options = SenderOptions.<String, String>create(config).maxInFlight(10000);

        var flux = Flux.range(0, 10)
                .map(i -> {
                    var headers = new RecordHeader("client_id", "   producer-1".getBytes());
                    var headers1 = new RecordHeader("request-type", "   producer-1-type".getBytes());
                    var record = new ProducerRecord<>("topic-1", "key-" + i, "value-" + i);
                    record.headers().add(headers);
                    record.headers().add(headers1);
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
