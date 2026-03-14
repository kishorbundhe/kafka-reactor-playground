package com.kafka.kafka_reactor_playground.concurrent;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

public class BasicSectionRunner {
    @SpringBootApplication(scanBasePackages = "com.kafka.kafka_reactor_playground.${section}.consumer")
    static class Consumer {
        public static void main(String[] args) {
            SpringApplication.run(Consumer.class,
                    "--section=concurrent",
                    "--config=simple-consumer");
        }
    }

    @SpringBootApplication(scanBasePackages = "com.kafka.kafka_reactor_playground.${section}.producer")
    static class Producer {
        public static void main(String[] args) {
            SpringApplication.run(Producer.class,
                    "--section=concurrent",
                    "--config=simple-producer");
        }
    }

    @SpringBootApplication(scanBasePackages = "com.kafka.kafka_reactor_playground.${section}.processor")
    static class Processor {
        public static void main(String[] args) {
            SpringApplication.run(Processor.class,
                    "--section=concurrent",
                    "--config=simple-processor-unordered",
                    "--processing-mode=ordered");
        }
    }

}