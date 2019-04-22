package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaCustomPartitioner {
    public static void main(String[] args) throws Exception {

        String topicName = "SensorTopic";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("partitioner.class", "CustomPartitioner");
        props.put("speed.sensor.name", "TSS");

        Producer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<>(topicName, "SSP" + i, "500" + i));

        for (int i = 0; i < 10; i++)
            producer.send(new ProducerRecord<>(topicName, "TSS", "500" + i));

        producer.close();
        System.out.println("SimpleProducer Completed.");
    }
}