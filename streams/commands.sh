./bin/zookeeper-server-start.sh conig/zookeeper.properties
./bin/kafka-server-start.sh conig/server.properties

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic streams-plaintext-input --partitions 1 --replication-factor 1

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic streams-wordcount-output --partitions 1 --replication-factor 1

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

./bin/kafka-console-producer.sh --bootstrap-server localhost:9092

./bin/kafka-console-producer.sh --broker-list 127.0.0.1:9092 --topic streams-plaintext-input
./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic streams-plaintext-intput --from-beginning

./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
   --topic word-count-output \
   --from-beginning \
   --formatter kafka.tools.DefaultMessageFormatter \
   --property print.key=true \
   --property print.value=true \
   --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
   --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer

./bin/kafka-run-class.sh org.apache.kafka.streams.examples.wordcount.WordCountDemo



./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic word-count-input --partitions 2 --replication-factor 1
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic word-count-output --partitions 2 --replication-factor 1