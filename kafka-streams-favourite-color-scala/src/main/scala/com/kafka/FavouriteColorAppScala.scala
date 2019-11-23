package com.kafka

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig._
import org.apache.kafka.streams.kstream.{KStream, KTable}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsBuilder}

object FavouriteColorAppScala {
  def main(args: Array[String]): Unit = {
    val bootstrapServers = "127.0.0.1:9092"
    val configId = "favourite-color-application"

    val config = new Properties
    config.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(APPLICATION_ID_CONFIG, configId)
    config.put(AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
    config.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)

    config.setProperty(CACHE_MAX_BYTES_BUFFERING_CONFIG, "0") //remove for prod

    val builder = new StreamsBuilder
    val textLines: KStream[String, String] = builder.stream("favourite-color-input")

    val usersAndColors = textLines
      .filter((_, value) => value.contains(","))
      .selectKey((_: String, value: String) => value.split(",")(0).toLowerCase)
      .mapValues((value: String) => value.split(",")(1).toLowerCase)
      .filter((_: String, color: String) => List("green", "blue", "red").contains(color))

    val intermediaryTopic = "user-keys-and-colors"
    usersAndColors.to(intermediaryTopic)

    val usersAndColorsTable: KTable[String, String] = builder.table(intermediaryTopic)

    val colorCounts = usersAndColorsTable
      .groupBy((_: String, color: String) => new KeyValue[String, String](color, color))
      .count

    colorCounts.toStream.to("favourite-color-output")

    val streams = new KafkaStreams(builder.build, config)
    streams.cleanUp() //remove on prod

    streams.start()
    System.out.println(streams.toString)

    Runtime.getRuntime.addShutdownHook(new Thread {
      override def run(): Unit = {
        streams.close()
      }
    })
  }
}
