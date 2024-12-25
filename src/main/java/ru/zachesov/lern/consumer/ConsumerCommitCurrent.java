package ru.zachesov.lern.consumer;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerCommitCurrent {

  public static void main(String[] args) {

    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(CommonClientConfigs.GROUP_ID_CONFIG, "CountryCounter");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {

      Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
      int count = 0;

      Duration timeout = Duration.ofMillis(100);
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf(
              "topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());
          currentOffsets.put(
              new TopicPartition(record.topic(), record.partition()),
              new OffsetAndMetadata(record.offset() + 1, "no metadata"));
          if (count % 1000 == 0) consumer.commitAsync(currentOffsets, null);
          count++;
        }
      }
    }
  }
}
