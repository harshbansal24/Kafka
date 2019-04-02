package com.kafka;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

/*
 * Partitioner will check if the messages last value is divisable by 3 then it will send the message to first partition otherwise to second partition
 *
 */
class SensorPartitioner implements Partitioner {

    private String speedSensorName;

    public void configure(Map<String, ?> configs) {
        speedSensorName = configs.get("speed.sensor.name").toString();
    }

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        int p = 0;
        if (Integer.parseInt(value.toString().split("-")[1]) % 3 == 0) {
            p = 0;
        } else {
            p = 1;
        }
        System.out.println("Key = " + (String) key + " Partition = " + p);
        return p;
    }

    public void close() {
    }
}
