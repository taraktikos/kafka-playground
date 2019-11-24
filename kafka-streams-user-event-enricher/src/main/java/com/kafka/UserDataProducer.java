package com.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class UserDataProducer {
    private static final String usersTopic = "user-table";
    private static final String userPurchasesTopic = "user-purchases";

    public static void main(String[] args) throws InterruptedException, ExecutionException {
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

        System.out.println("\nExample 1 - new user\n");
        producer.send(userRecord("john", "First=John,Last=Doe,Email=mail")).get();
        producer.send(purchaseRecord("john", "Apples and bananas (1)")).get();
        Thread.sleep(10000);

        System.out.println("\nExample 2 - non existing user\n");
        producer.send(purchaseRecord("bob", "Test purchase (2)")).get();
        Thread.sleep(10000);

        System.out.println("\nExample 3 - update to user\n");
        producer.send(userRecord("john", "First=Johny,Last=Updated Doe,Email=mail222")).get();
        producer.send(purchaseRecord("john", "Orange (3)")).get();
        Thread.sleep(10000);

        System.out.println("\nExample 4\n");
        producer.send(purchaseRecord("stephane", "Computer (4)")).get();
        producer.send(userRecord("stephane", "First=Stephane,Last=Last,Email=mail2")).get();
        producer.send(purchaseRecord("stephane", "Book (4)")).get();
        producer.send(userRecord("stephane", null)).get();
        Thread.sleep(10000);

        System.out.println("\nExample 5\n");
        producer.send(userRecord("alice", "First=Alice,Last=Alice,Email=mail2")).get();
        producer.send(userRecord("alice", null)).get();
        producer.send(purchaseRecord("alice", "some stuff (5)")).get();
        Thread.sleep(10000);

        producer.close();
    }

    private static ProducerRecord<String, String> purchaseRecord(String name, String purchaseInfo) {
        return new ProducerRecord<>(userPurchasesTopic, name, purchaseInfo);
    }

    private static ProducerRecord<String, String> userRecord(String name, String info) {
        return new ProducerRecord<>(usersTopic, name, info);
    }
}
