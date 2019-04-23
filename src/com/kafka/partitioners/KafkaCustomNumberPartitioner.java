package com.kafka.partitioners;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class KafkaCustomNumberPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        System.out.println("number of parition " + numPartitions);
        int valuePassed = Integer.parseInt(value.toString());
        if (valuePassed % 3 == 0) {
            return 0;
        } else if (valuePassed % 5 == 0) {
            return 1;

        } else {
            return 2;
        }

//        Utils.toArray(Utils.murmur2(valueBytes));
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
