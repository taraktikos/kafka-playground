package com.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

import static java.util.Arrays.asList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.*;

/*
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic favourite-color-input --partitions 1 --replication-factor 1
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-keys-and-colors --partitions 1 --replication-factor 1 --config cleanup.policy=compact
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic favourite-color-output --partitions 1 --replication-factor 1 --config cleanup.policy=compact

./bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic favourite-color-input
stephane,blue
john,green
stephane,red
alice,red

./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
   --topic favourite-color-output \
   --from-beginning \
   --formatter kafka.tools.DefaultMessageFormatter \
   --property print.key=true \
   --property print.value=true \
   --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
   --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

blue,0
green,1
red,2
 */

public class FavouriteColorApp {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String configId = "favourite-color-application";

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(APPLICATION_ID_CONFIG, configId);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        //remove on prod
        properties.setProperty(CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream("favourite-color-input");

        final KStream<String, String> usersAndColors = textLines
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                .mapValues(value -> value.split(",")[1].toLowerCase())
                .filter((user, color) -> asList("green", "blue", "red").contains(color));

        usersAndColors.to("user-keys-and-colors");

        final KTable<String, String> usersAndColorsTable = builder.table("user-keys-and-colors");

        final KTable<String, Long> colorCounts = usersAndColorsTable
                .groupBy((user, color) -> new KeyValue<>(color, color))
                .count();

        colorCounts.toStream().to("favourite-color-output", Produced.with(Serdes.String(), Serdes.Long()));

        KafkaStreams streams = new KafkaStreams(builder.build(), properties);
        streams.cleanUp(); //remove on prod
        streams.start();
        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
