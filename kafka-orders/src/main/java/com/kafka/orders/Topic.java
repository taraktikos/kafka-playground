package com.kafka.orders;

import com.kafka.orders.serializers.ProductTypeSerde;
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
        public static Topic<Product, Integer> INVENTORY = new Topic<>("warehouse-inventory", new ProductTypeSerde(), Serdes.Integer());
    }
}
