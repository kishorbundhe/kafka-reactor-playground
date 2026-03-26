package com.kafka.kafka_reactor_playground.playandpause;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;


public class BasicSectionRunner {
    @SpringBootApplication(scanBasePackages = "com.kafka.kafka_reactor_playground.${section}.consumer")
    @EnableScheduling
    static class Consumer {
         public static void main(String[] args) {
            SpringApplication.run(Consumer.class,
                    "--section=playandpause",
                    "--config=simple-consumer"
            );
        }
    }

    @SpringBootApplication(scanBasePackages = "com.kafka.kafka_reactor_playground.${section}.producer")
    static class Producer {
          public static void main( String[] args) {
            SpringApplication.run(Producer.class,
                    "--section=playandpause",
                    "--config=simple-producer"
            );
        }
    }

}