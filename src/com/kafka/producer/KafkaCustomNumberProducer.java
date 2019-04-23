package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;


/**
 * any number divisable should be published to partition 1
 * any number divisable by 5 should be published to partition 2
 * any number not divisable by any of above criteria should be in partition 3
 */
public class KafkaCustomNumberProducer {
    public static void main(String[] args) throws Exception {

        String topicName = "test-KafkaHarsh";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "com.kafka.partitioners.KafkaCustomNumberPartitioner");

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<>(topicName, "SSP" + i, "500" + i));

        producer.close();
        System.out.println("SimpleProducer Completed.");
    }
}