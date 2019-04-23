package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaRebalancingProducer {
    public static void main(String args[]) {
        String key1 = "Key1";
        String key2 = "Key2";
        String key3 = "Key3";
        String value1 = "Value-Partition1-";
        String value2 = "Value-Partition2-";
        String topicName = "test-KafkaHarsh";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i <= 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, 0, key1, value1 + i);
            ProducerRecord<String, String> record2 = new ProducerRecord<>(topicName, 1, key2, value2 + i);
            ProducerRecord<String, String> record3 = new ProducerRecord<>(topicName, 2, key3, value2 + i);
            producer.send(record);
            producer.send(record2);
            producer.send(record3);
            System.out.println("Published!!");
        }
        producer.close();
        System.out.println("Simple Producer Closed");
    }
}
