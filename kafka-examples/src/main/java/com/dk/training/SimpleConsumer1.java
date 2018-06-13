package com.dk.training;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

//version 0.10.0.1
public class SimpleConsumer1 {

	public static void main(String[] args) {
		// Kafka consumer configuration settings
		String topicName = "dk-topic";
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		// props.put("auto.commit.interval.ms", "1000");
		// props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", StringDeserializer.class.getName());
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		// Kafka Consumer subscribes list of topics here.
		consumer.subscribe(Arrays.asList(topicName));

		// print the topic name
		System.out.println("Subscribed to topic " + topicName);

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {

				// print the offset,key and value for the consumer records.
				System.out.printf("offset = %d, key = %s, value = %s\n", record.offset(), record.key(), record.value());
				
			}

		}

	}

}
