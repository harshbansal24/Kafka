package com.kafka.sample;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerSample {
    public static void main(String args[]) {
        String key = "Key1";
        String value = "Value-";
        String topicName = "test-KafkaHarsh";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i <= 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value+i);
            producer.send(record);
            System.out.println("Published!!");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
        System.out.println("Simple Producer Closed");
    }
}
