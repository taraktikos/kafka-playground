package com.kafka.orders.services;

import com.kafka.orders.Order;
import com.kafka.orders.Topic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.math.BigDecimal;
import java.util.Properties;

import static com.kafka.orders.Order.State.CREATED;
import static com.kafka.orders.Product.UNDERPANTS;
import static com.kafka.orders.Topic.Topics.ORDERS;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

public class OrderGenerator {
    public static void main(String[] args) {
        final Properties config = createConfig();
        final Topic<String, Order> topic = ORDERS;


        final KafkaProducer<String, Order> producer = new KafkaProducer<>(config, topic.getKeySerde().serializer(), topic.getValueSerde().serializer());
        while (true) {
            final Order order = new Order("0", 20L, CREATED, UNDERPANTS, 3, BigDecimal.valueOf(5.00));
            final ProducerRecord<String, Order> record = new ProducerRecord<>(topic.getName(), order.getId(), order);
            producer.send(record);
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    private static Properties createConfig() {
        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ACKS_CONFIG, "all");
        properties.put(CLIENT_ID_CONFIG, "order-generator");
        properties.put(RETRIES_CONFIG, 0);
        return properties;
    }
}
