package com.kafka;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class BankTransactionsProducer {
    private static final String topic = "bank-transactions";

    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";

        Properties props = new Properties();
        props.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.setProperty(ACKS_CONFIG, "all");
        props.setProperty(RETRIES_CONFIG, "3");
        props.setProperty(LINGER_MS_CONFIG, "1");
        props.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer<String, String> producer = new KafkaProducer<>(props);

        int i = 0;
        while (true) {
            System.out.println("Producing batch: " + i);
            try {
                producer.send(newRandomTransaction("john"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("stephane"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("alice"));
                Thread.sleep(100);
                i++;
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    private static ProducerRecord<String, String> newRandomTransaction(String name) {
        final ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        final Integer amount = ThreadLocalRandom.current().nextInt(0, 100);
        final Instant now = Instant.now();
        transaction.put("name", name);
        transaction.put("amount", amount);
        transaction.put("time", now.toString());
        return new ProducerRecord<>(topic, name, transaction.toString());
    }
}
