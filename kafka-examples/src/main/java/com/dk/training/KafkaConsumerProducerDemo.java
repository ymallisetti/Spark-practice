package com.dk.training;



public class KafkaConsumerProducerDemo {
    public static void main(String[] args) {
        boolean isAsync = args.length == 0 || !args[0].trim().equalsIgnoreCase("sync");
        Producer producerThread = new Producer(KafkaProperties.TOPIC, isAsync);
        producerThread.start();

        Consumer2 consumerThread = new Consumer2(KafkaProperties.TOPIC);
        consumerThread.start();
        
        

    }
}