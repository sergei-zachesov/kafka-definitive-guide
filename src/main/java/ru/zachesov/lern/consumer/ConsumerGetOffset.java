package ru.zachesov.lern.consumer;

import java.time.Instant;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class ConsumerGetOffset {
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(CommonClientConfigs.GROUP_ID_CONFIG, "CountryCounter");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    long oneHourEarlier =
        Instant.now().atZone(ZoneId.systemDefault()).minusHours(1).toEpochSecond();
    Map<TopicPartition, Long> partitionTimestampMap =
        consumer.assignment().stream().collect(Collectors.toMap(tp -> tp, tp -> oneHourEarlier));
    Map<TopicPartition, OffsetAndTimestamp> offsetMap =
        consumer.offsetsForTimes(partitionTimestampMap);
    for (Map.Entry<TopicPartition, OffsetAndTimestamp> entry : offsetMap.entrySet()) {
      consumer.seek(entry.getKey(), entry.getValue().offset());
    }
  }
}
