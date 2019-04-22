package com.kafka;

import org.apache.kafka.clients.producer.*;

import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaAsynchronousProducer {

    public static void main(String args[]) {
        {
            String key = "Key1";
            String value = "Async - Value-";
            String topicName = "test-KafkaHarsh";

            Properties props = new Properties();
            props.put("bootstrap.servers", "localhost:9092,localhost:9093");
            props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
            props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

            Producer<String, String> producer = new KafkaProducer<>(props);

            for (int i = 0; i <= 10; i++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, value + i + new Date());
                try {
                    Thread.sleep(1000);

                    producer.send(record, (metadata, exception) -> {
                        System.out.println("Published !!" + metadata.partition() + " " + metadata.offset());
                    });
//                    producer.send(record, new Callback() {
//                        @Override
//                        public void onCompletion(RecordMetadata metadata, Exception exception) {
//                            System.out.println("Published !!" + metadata.partition() + " " + metadata.offset());
//                        }
//                    });
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            producer.close();
            System.out.println("Simple Producer Closed");
        }

    }
}
