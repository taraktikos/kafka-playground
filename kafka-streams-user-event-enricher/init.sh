./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-purchases --partitions 3 --replication-factor 1

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-table --partitions 2 --replication-factor 1 --config cleanup.policy=compact

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-purchases-enriched-inner-join --partitions 3 --replication-factor 1

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-purchases-enriched-left-join --partitions 3 --replication-factor 1

./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
   --topic user-purchases-enriched-left-join \
   --from-beginning \
   --formatter kafka.tools.DefaultMessageFormatter \
   --property print.key=true \
   --property print.value=true \
   --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
   --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
   --topic user-purchases-enriched-inner-join \
   --from-beginning \
   --formatter kafka.tools.DefaultMessageFormatter \
   --property print.key=true \
   --property print.value=true \
   --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
   --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
