./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bank-transactions --partitions 1 --replication-factor 1

./bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic bank-balance-exactly-once --partitions 1 --replication-factor 1

./bin/kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 \
   --topic bank-balance-exactly-once \
   --from-beginning \
   --formatter kafka.tools.DefaultMessageFormatter \
   --property print.key=true \
   --property print.value=true \
   --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
   --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
