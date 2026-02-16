package com.kafka.kafka_reactor_playground.sec03;

public class KafkaConsumerGroup {
   public static class Consumer01{
     public static void main(String[] args) {
        KafkaConsumer.create(1);
     }
   }

   public static class Consumer02{
     public static void main(String[] args) {
        KafkaConsumer.create(2);
     }
   }

   public static class Consumer3{
     public static void main(String[] args) {
        KafkaConsumer.create(3);
     }
   }
}
