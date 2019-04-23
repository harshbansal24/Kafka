package com.kafka.consumer;

import com.kafka.listener.RebalanceListener;
import com.kafka.model.Employee;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.RebalanceInProgressException;

import java.util.Arrays;
import java.util.Properties;

public class KafkaRebalancingConsumer {

    public static void main(String[] args) {
        String topicName = "test-KafkaHarsh";
        String strGroupID = "group-KafkaHarsh";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("group.id", strGroupID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("enable.auto.commit", "false");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        RebalanceListener rebalanceListener = new RebalanceListener(kafkaConsumer);

        kafkaConsumer.subscribe(Arrays.asList(topicName));

        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("Employee ID " + String.valueOf(record.value()) + " partition " + record.partition());
                    rebalanceListener.addOffSet(record.topic(), record.partition(), record.offset());
                }
                kafkaConsumer.commitSync(rebalanceListener.getCurrentOffsets());
            }
        } catch (Exception ex) {
            System.out.println("Exception ");
            ex.printStackTrace();
        } finally {
            kafkaConsumer.close();
        }
    }
}
