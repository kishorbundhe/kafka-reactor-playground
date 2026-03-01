package com.kafka.kafka_reactor_playground.basic.producer;

import org.springframework.boot.CommandLineRunner;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.stereotype.Component;

import java.util.logging.Logger;

@Component
public class PingProducer implements CommandLineRunner {
    private final Logger logger = Logger.getLogger(PingProducer.class.getName());
    private final StreamBridge streamBridge;

    public PingProducer(StreamBridge streamBridge) {
        this.streamBridge = streamBridge;
    }

    @Override
    public void run(String... args) throws Exception {
        ProcessBuilder processBuilder = new ProcessBuilder("ping", "-c", "10", "google.com")
                .redirectErrorStream(true);
        Process process = processBuilder.start();
        try (var result = process.inputReader()) {
            result.lines().forEach(

                    line -> {
                        logger.info("msg : "+line);
                        streamBridge.send("ping-out", line);
                    }
            );
        }
    }
}
