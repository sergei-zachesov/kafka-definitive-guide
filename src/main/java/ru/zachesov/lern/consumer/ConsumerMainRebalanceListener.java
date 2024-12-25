package ru.zachesov.lern.consumer;

import java.time.Duration;
import java.util.*;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class ConsumerMainRebalanceListener {

  private static final KafkaConsumer<String, String> consumer = getConsumer();
  private static final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
  private static final Duration timeout = Duration.ofMillis(100);

  public static void main(String[] args) {

    try {
      consumer.subscribe(List.of("topic"), new HandleRebalance());
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf(
              "topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());
          currentOffsets.put(
              new TopicPartition(record.topic(), record.partition()),
              new OffsetAndMetadata(record.offset() + 1, null));
        }
        consumer.commitAsync(currentOffsets, null);
      }
    } catch (WakeupException e) {
      // Игнорируем, поскольку закрываемся
    } catch (Exception e) {
      log.error("Unexpected error", e);
    } finally {
      try {
        consumer.commitSync(currentOffsets);
      } finally {
        consumer.close();
        System.out.println("Closed consumer and we are done");
      }
    }
  }

  private static KafkaConsumer<String, String> getConsumer() {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(CommonClientConfigs.GROUP_ID_CONFIG, "CountryCounter");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    return new KafkaConsumer<>(props);
  }

  private static class HandleRebalance implements ConsumerRebalanceListener {
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {}

    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
      System.out.println(
          "Lost partitions in rebalance. " + "Committing current offsets:" + currentOffsets);
      consumer.commitSync(currentOffsets);
    }
  }
}
