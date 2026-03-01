package com.kafka.kafka_reactor_playground;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


public class BasicSectionRunner {
    @SpringBootApplication(scanBasePackages = "com.kafka.kafka_reactor_playground.${section}.consumer")
    static class Consumer {
         public static void main(String[] args) {
            SpringApplication.run(Consumer.class,
                    "--section=basic",
                    "--config=simple-consumer"
            );
        }
    }

    @SpringBootApplication(scanBasePackages = "com.kafka.kafka_reactor_playground.${section}.producer")
    static class Producer {
          public static void main( String[] args) {
            SpringApplication.run(Producer.class,
                    "--section=basic",
                    "--config=simple-producer"
            );
        }
    }

}