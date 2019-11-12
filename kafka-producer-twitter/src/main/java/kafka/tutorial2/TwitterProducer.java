package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static com.twitter.hbc.core.Constants.STREAM_HOST;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class TwitterProducer {
    private final Logger log = LoggerFactory.getLogger(TwitterProducer.class);

    List<String> terms = Lists.newArrayList("usa", "bitcoin", "politics", "sport");

    public TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        KafkaProducer<String, String> producer = createKafkaProducer();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Stopping app");
            client.stop();
            producer.close();
        }));

        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                log.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (metadata, e) -> {
                    if (e == null) {
                        log.info("Received new metadata.\n" +
                                "Topic: " + metadata.topic() + "\n" +
                                "Partition: " + metadata.partition() + "\n" +
                                "Offset: " + metadata.offset() + "\n" +
                                "Timestamp: " + metadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                });
            }
        }
        log.info("End of app");
    }

    public KafkaProducer<String, String> createKafkaProducer() {
        String bootstrapServers = "127.0.0.1:9092";

        Properties properties = new Properties();
        properties.setProperty(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // create safe Producer
        properties.setProperty(ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ACKS_CONFIG, "all");
        properties.setProperty(RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        //high throughput producer
        properties.setProperty(COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(LINGER_MS_CONFIG, "20");
        properties.setProperty(BATCH_SIZE_CONFIG, Integer.toString(32 * 1024)); // 32KB batch size

        return new KafkaProducer<>(properties);
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosts = new HttpHosts(STREAM_HOST);
        StatusesFilterEndpoint statusesFilterEndpoint = new StatusesFilterEndpoint();
        statusesFilterEndpoint.trackTerms(terms);
        Authentication auth = new OAuth1("ntk2wBoK3X1hdlokZm88slsNS", "JYg8LatzsLYEdCJ7Ma2SDBCX4k8lZ660ayjp5ad0SUai0Sw3QN",
                "253072669-wR4jfBmMzXkgTIlR5EaqdLs3S6xOt9NIbJjbHiCl", "j3r4HdNqQmzLjXVVm5Mt5JLnOgTIdMzdyZF1xObYvLJCu");

        ClientBuilder builder = new ClientBuilder()
                .name("Twitter-client-01")
                .hosts(hosts)
                .authentication(auth)
                .endpoint(statusesFilterEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
        return builder.build();
    }
}
