package com.kafka.kafka_reactor_playground.processor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


public class BasicSectionRunner {
    @SpringBootApplication(scanBasePackages = "com.kafka.kafka_reactor_playground.${section}.consumer")
    static class Consumer {
         public static void main(String[] args) {
            SpringApplication.run(Consumer.class,
                    "--section=processor",
                    "--config=simple-consumer"
            );
        }
    }

    @SpringBootApplication(scanBasePackages = "com.kafka.kafka_reactor_playground.${section}.producer")
    static class Producer {
          public static void main( String[] args) {
            SpringApplication.run(Producer.class,
                    "--section=processor",
                    "--config=simple-producer"
            );
        }
    }

    @SpringBootApplication(scanBasePackages = "com.kafka.kafka_reactor_playground.${section}.process")
    static class Processor {
         public static void main(String[] args) {
            SpringApplication.run(Processor.class,
                    "--section=processor",
                    "--config=simple-processor"
            );
        }
    }

}