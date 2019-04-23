package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaSynchronousProducer {
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
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value + i + new Date());
            try {
                RecordMetadata metadata = producer.send(record).get();
                System.out.println("Published!! with status" + metadata.offset());
                Thread.sleep(1000);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }
        producer.close();
        System.out.println("Simple Producer Closed");
    }
}
