package com.kafka.producer;

import com.kafka.model.Employee;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


/**
 * any number divisable should be published to partition 1
 * any number divisable by 5 should be published to partition 2
 * any number not divisable by any of above criteria should be in partition 3
 */
public class KafkaSerializerProducer {
    public static void main(String[] args) throws Exception {

        String topicName = "test-KafkaHarsh";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "com.kafka.seralizer.CustomSerializer");

        Producer<String, Employee> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<>(topicName, "" + i, new Employee(i, "Harsh" + i, "IBIT" + i)));

        producer.close();
        System.out.println("KafkaSerializerProducer Completed.");
    }
}