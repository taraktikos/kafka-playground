package com.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Instant;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

public class BankBalanceExactlyOnceApp {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String configId = "bank-balance-application";

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(APPLICATION_ID_CONFIG, configId);
        props.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Exactly once processing!
        props.put(PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE);
        //remove on prod
        props.put(CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        final JsonSerializer jsonSerializer = new JsonSerializer();
        final JsonDeserializer jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, JsonNode> bankTransactions = builder.stream("bank-transactions", Consumed.with(Serdes.String(), jsonSerde));

        final ObjectNode initialBalance = JsonNodeFactory.instance.objectNode();
        initialBalance.put("count", 0);
        initialBalance.put("amount", 0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());

        final KTable<String, JsonNode> bankBalance = bankTransactions
                .groupByKey(Grouped.with(Serdes.String(), jsonSerde))
                .aggregate(
                        () -> initialBalance,
                        (key, transaction, balance) -> newBalance(transaction, balance),
                        Materialized.with(Serdes.String(), jsonSerde)
                );

        bankBalance.toStream().to("bank-balance-exactly-once", Produced.with(Serdes.String(), jsonSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.cleanUp(); //remove on prod
        streams.start();
        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode newBalance(JsonNode transaction, JsonNode balance) {
        final ObjectNode newBalance = JsonNodeFactory.instance.objectNode();
        newBalance.put("count", balance.get("count").asInt() + 1);
        newBalance.put("amount", balance.get("amount").asInt() + transaction.get("amount").asInt());

        final Long balanceEpoch = Instant.parse(balance.get("time").asText()).toEpochMilli();
        final Long transactionEpoch = Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant = Instant.ofEpochMilli(Math.max(balanceEpoch, transactionEpoch));
        newBalance.put("time", newBalanceInstant.toString());
        return newBalance;
    }
}
