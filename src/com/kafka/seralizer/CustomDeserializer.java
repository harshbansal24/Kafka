package com.kafka.seralizer;

import com.kafka.model.Employee;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Map;

public class CustomDeserializer implements Deserializer<Employee> {
    private String encoding = "UTF-8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public Employee deserialize(String topic, byte[] data) {
        ByteBuffer byteBuffer = ByteBuffer.wrap(data);
        int id = byteBuffer.getInt();
        Employee e = new Employee(id, null, null);
        return e;
    }

    @Override
    public void close() {
    }
}
