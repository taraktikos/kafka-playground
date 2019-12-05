package com.kafka.orders.serializers;

import com.kafka.orders.Product;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class ProductSerde implements Serde<Product> {
    @Override
    public Serializer<Product> serializer() {
        return (topic, data) -> data.toString().getBytes();
    }

    @Override
    public Deserializer<Product> deserializer() {
        return (topic, data) -> Product.valueOf(new String(data));
    }
}
