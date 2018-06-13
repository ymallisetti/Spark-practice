package com.dk.training;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

//version = 0.10.0.1
public class SimpleProcedure1 {

	public static void main(String[] args) {

		String topicName = "dk-topic";

		// properties for producer
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", StringSerializer.class.getName());
		props.put("value.serializer", StringSerializer.class.getName());

		// create producer
		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		// send messages to my-topic
		for (int i = 0; i < 10; i++) {
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topicName,
					Integer.toString(i), "DK2 msg #" + Integer.toString(i));
			producer.send(producerRecord);
		}

		// close producer
		producer.close();

	}

}
