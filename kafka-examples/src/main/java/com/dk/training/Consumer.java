package com.dk.training;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

//import kafka.utils.ShutdownableThread;

public class Consumer extends Thread {
	private final KafkaConsumer<Integer, String> consumer;
	private final String topic;

	public Consumer(String topic) {
		// super("KafkaConsumerExample", false);
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.IntegerDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");

		consumer = new KafkaConsumer<>(props);
		this.topic = topic;
	}

	@Override
	public void run() {
		System.out.println("Consumer started :: ");
		consumer.subscribe(Collections.singletonList(this.topic));
		while (true) {
			ConsumerRecords<Integer, String> records = consumer.poll(3000);
			for (ConsumerRecord<Integer, String> record : records) {
				
				System.out.println("Received message: (" + record.key() + ", " + record.value() + ") at offset "
						+ record.offset());
				String[] lineValues = record.value().split(",");
				
				System.out.println(lineValues[1] +": current price --> "+lineValues[3]);
				
			}
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/*
	 * @Override public String name() { return null; }
	 * 
	 * @Override public boolean isInterruptible() { return false; }
	 */
}