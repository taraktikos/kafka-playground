package kafka.tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;


public class StreamsFilterTweets {
    public static void main(String[] args) {
        String bootstrapServers = "127.0.0.1:9092";
        String configId = "demo-kafka-streams";

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(APPLICATION_ID_CONFIG, configId);
        properties.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        final KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
        // filter for tweets which has a user of over 10000 followers
        final KStream<String, String> filteredStream = inputTopic.filter((k, json) -> extractUserFollowersInTweet(json) > 10000);
        filteredStream.to("important_tweets");

        final KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

        kafkaStreams.start();
    }

    private static Integer extractUserFollowersInTweet(String tweetJson) {
        try {
            return JsonParser.parseString(tweetJson)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("followers_count")
                    .getAsInt();
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
