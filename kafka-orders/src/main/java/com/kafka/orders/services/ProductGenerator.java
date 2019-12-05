package com.kafka.orders.services;

import com.kafka.orders.Pair;
import com.kafka.orders.Product;
import com.kafka.orders.Topic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.kafka.orders.Topic.Topics.INVENTORY;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

public class ProductGenerator {
    public static void main(String[] args) {
        final Properties config = createConfig();
        final Topic<Product, Integer> topic = INVENTORY;

        final List<Pair<Product, Integer>> inventory = Arrays.asList(Pair.of(Product.UNDERPANTS, 20), Pair.of(Product.JUMPERS, 10));

        final KafkaProducer<Product, Integer> stockProducer = new KafkaProducer<>(config, topic.getKeySerde().serializer(), topic.getValueSerde().serializer());
        for (Pair<Product, Integer> kv : inventory) {
            stockProducer.send(new ProducerRecord<>(topic.getName(), kv.getKey(), kv.getValue()));
        }

        Runtime.getRuntime().addShutdownHook(new Thread(stockProducer::close));
    }

    private static Properties createConfig() {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ACKS_CONFIG, "all");
        properties.put(CLIENT_ID_CONFIG, "product-generator");
        properties.put(RETRIES_CONFIG, 0);
        return properties;
    }
}
