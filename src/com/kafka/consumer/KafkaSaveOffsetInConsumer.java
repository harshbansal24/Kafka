package com.kafka.consumer;

import com.kafka.model.Employee;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

public class KafkaSaveOffsetInConsumer {

    public static void main(String[] args) {
        String topicName = "test-KafkaHarsh";
        String strGroupID = "group-KafkaHarsh";

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093");
        props.put("group.id", strGroupID);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "com.kafka.seralizer.CustomDeserializer");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(props);

        // Creating Topic Partition
        TopicPartition topicPartition1 = new TopicPartition(topicName, 0);
        TopicPartition topicPartition2 = new TopicPartition(topicName, 1);
        TopicPartition topicPartition3 = new TopicPartition(topicName, 2);

        // All topics are assigned to this consumer only.
        kafkaConsumer.assign(Arrays
                .asList(topicPartition1, topicPartition2, topicPartition3));

        kafkaConsumer.seek(topicPartition1, getOffsetfor(0));
        kafkaConsumer.seek(topicPartition2, getOffsetfor(1));
        kafkaConsumer.seek(topicPartition3, getOffsetfor(2));
        int count = 0;
        do {
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(1000);
            count = poll.count();
            for (ConsumerRecord<String, String> record : poll) {
                processAndSaveRecordOffset(record);
            }
        } while (count > 0);

    }

    /*
     * This method will process the record and save the state so that in next iteration, we are able to read from last read offset. Both processing and saving offset would be atomic operation
     */
    private static void processAndSaveRecordOffset(ConsumerRecord<String, String> record) {
    }

    /**
     * This method will get the offset from database.
     *
     * @param i
     * @return
     */
    private static long getOffsetfor(int i) {
        return 0;
    }

}
