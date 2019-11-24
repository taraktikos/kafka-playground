package com.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.GlobalKTable;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

public class UserEventEnricherApp {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String configId = "user-event-enricher-application";

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(APPLICATION_ID_CONFIG, configId);
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder builder = new StreamsBuilder();
        final GlobalKTable<String, String> usersGlobalTable = builder.globalTable("user-table");

        final KStream<String, String> userPurchases = builder.stream("user-purchases");

        final KStream<String, String> userPurchasesEnrichedJoin = userPurchases.join(
                usersGlobalTable,
                (key, value) -> key,
                (userPurchase, userInfo) -> "Purchase=" + userPurchase + ", UserInfo=[" + userInfo + "]"
        );
        userPurchasesEnrichedJoin.to("user-purchases-enriched-inner-join");

        final KStream<String, String> userPurchasesEnrichedLeftJoin = userPurchases.leftJoin(
                usersGlobalTable,
                (key, value) -> key,
                (userPurchase, userInfo) -> {
                    if (userInfo != null) {
                        return "Purchase=" + userPurchase + ", UserInfo=[" + userInfo + "]";
                    } else {
                        return "Purchase=" + userPurchase + ", UserInfo=null";
                    }
                }
        );
        userPurchasesEnrichedLeftJoin.to("user-purchases-enriched-left-join");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp(); //remove on prod
        streams.start();
        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
