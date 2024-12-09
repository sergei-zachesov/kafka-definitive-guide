package ru.zachesov.lern.cunsumer;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

@Slf4j
public class ConsumerCommitAsyncAndSync {

  public static volatile boolean closing = false;

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(CommonClientConfigs.GROUP_ID_CONFIG, "CountryCounter");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
      Duration timeout = Duration.ofMillis(100);
      while (!closing) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf(
              "topic = %s, partition = %s, offset = %d, customer = %s, country = %s",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());
        }
        consumer.commitAsync();
      }
      consumer.commitSync();
    } catch (Exception e) {
      log.error("Unexpected error", e);
    }
  }
}
