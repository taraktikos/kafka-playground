package com.kafka;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Properties;

import static org.apache.kafka.streams.StreamsConfig.*;
import static org.apache.kafka.streams.test.OutputVerifier.compareKeyValue;
import static org.junit.Assert.assertNull;

public class WordCountAppTest {
    StringSerializer stringSerializer = new StringSerializer();
    ConsumerRecordFactory<String, String> factory = new ConsumerRecordFactory<>(stringSerializer, stringSerializer);

    TopologyTestDriver testDriver;

    @Before
    public void setUp() {
        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "test");
        props.put(BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.setProperty(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.setProperty(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        final WordCountApp wordCountApp = new WordCountApp();
        testDriver = new TopologyTestDriver(wordCountApp.createTopology(), props);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    @Test
    public void countsTest() {
        pushNewRecord("Testing kafka streams");
        compareKeyValue(readOutput(), "testing", 1L);
        compareKeyValue(readOutput(), "kafka", 1L);
        compareKeyValue(readOutput(), "streams", 1L);
        assertNull(readOutput());
        pushNewRecord("Testing kafka streams2");
        compareKeyValue(readOutput(), "testing", 2L);
        compareKeyValue(readOutput(), "kafka", 2L);
        compareKeyValue(readOutput(), "streams2", 1L);
        assertNull(readOutput());
    }

    public ProducerRecord<String, Long> readOutput() {
        return testDriver.readOutput("word-count-output", new StringDeserializer(), new LongDeserializer());
    }

    public void pushNewRecord(String value) {
        testDriver.pipeInput(factory.create("word-count-input", null, value));
    }
}
