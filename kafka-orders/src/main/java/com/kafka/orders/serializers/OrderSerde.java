package com.kafka.orders.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.orders.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;

@Slf4j
@RequiredArgsConstructor
public class OrderSerde implements Serde<Order> {

    private final ObjectMapper objectMapper;

    @Override
    public Serializer<Order> serializer() {
        return (topic, data) -> {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (JsonProcessingException e) {
                log.error("Can't serialize order", e);
                return null;
            }
        };
    }

    @Override
    public Deserializer<Order> deserializer() {
        return (topic, data) -> {
            try {
                return objectMapper.readValue(data, Order.class);
            } catch (IOException e) {
                log.error("Can't deserialize order", e);
                return null;
            }
        };
    }
}
