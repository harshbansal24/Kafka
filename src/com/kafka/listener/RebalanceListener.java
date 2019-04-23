package com.kafka.listener;

import com.kafka.model.Employee;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class RebalanceListener implements ConsumerRebalanceListener {
    KafkaConsumer<String, String> kafkaConsumer;
    private Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap();

    public RebalanceListener(KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public void addOffSet(String topic, int partition, long offset) {
        currentOffsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset, "Commit"));
    }

    public Map<TopicPartition, OffsetAndMetadata> getCurrentOffsets() {
        return currentOffsets;
    }


    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("Following Partitions Revoked ....");
        for (TopicPartition topicPartition : partitions) {
            System.out.println(topicPartition.partition()+",");
        }

        System.out.println("Following Partitions Committed .... ");
        for (TopicPartition topicPartition : currentOffsets.keySet()){
            System.out.println(topicPartition.partition());
        }

        kafkaConsumer.commitSync(currentOffsets);
        currentOffsets.clear();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("Following Partitions Assigned ....");
        for (TopicPartition topicPartition : partitions)
            System.out.println(topicPartition.partition() + ",");
    }
}
