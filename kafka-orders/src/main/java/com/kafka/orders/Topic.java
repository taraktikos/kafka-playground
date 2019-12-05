package com.kafka.orders;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.orders.serializers.OrderSerde;
import com.kafka.orders.serializers.ProductSerde;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

@Data
@AllArgsConstructor
public class Topic<K, V> {
    private String name;
    private Serde<K> keySerde;
    private Serde<V> valueSerde;

    public static class Topics {
        public static Topic<Product, Integer> INVENTORY = new Topic<>("warehouse-inventory", new ProductSerde(), Serdes.Integer());
        public static Topic<String, Order> ORDERS = new Topic<>("orders", Serdes.String(), new OrderSerde(new ObjectMapper()));
    }
}
