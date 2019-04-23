package com.kafka.seralizer;

import com.kafka.model.Employee;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CustomSerializer implements Serializer<Employee> {
    private String encoding = "UTF-8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, Employee data) {
        try {
            byte[] bytes = data.getName().getBytes(encoding);
            ByteBuffer byteBuffer = ByteBuffer.allocate(bytes.length);
            return byteBuffer.array();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {
    }
}
