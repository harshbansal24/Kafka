package com.kafka.consumer;

import com.kafka.model.Employee;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaNumberConsumer {

    public static void main(String[] args) {
        String topicName = "test-KafkaHarsh";
        String strGroupID = "group-KafkaHarsh";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("group.id", strGroupID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.kafka.seralizer.CustomDeserializer");

        KafkaConsumer<String, Employee> kafkaConsumer = new KafkaConsumer<>(props);

        kafkaConsumer.subscribe(Arrays.asList(topicName));

        while (true) {
            ConsumerRecords<String, Employee> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, Employee> record : records) {
                System.out.println("Employee ID " + String.valueOf(record.value().getId()) + " partition " + record.partition());
            }
        }
    }
}
