package com.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Properties;

public class KafkaStream {
    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafkaHarshStream");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder streamsBuilder = new StreamsBuilder();
//        KStream<Object, Object> kafkaHarshStream = streamsBuilder.stream("test-KafkaHarsh");
        streamsBuilder.stream("test-KafkaHarsh").to("kafkaHarshStream");

        final Topology topology = streamsBuilder.build();
        System.out.println(topology.describe());

        
    }
}
