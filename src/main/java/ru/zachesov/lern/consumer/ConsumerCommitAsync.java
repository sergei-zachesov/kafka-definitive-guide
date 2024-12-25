package ru.zachesov.lern.consumer;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

public class ConsumerCommitAsync {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(CommonClientConfigs.GROUP_ID_CONFIG, "CountryCounter");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    try {
      AtomicInteger atomicInteger = new AtomicInteger(0);
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(4));
        for (ConsumerRecord<String, String> record : records) {
          System.out.format("offset: %d\n", record.offset());
          System.out.format("partition: %d\n", record.partition());
          System.out.format("timestamp: %d\n", record.timestamp());
          System.out.format("timeStampType: %s\n", record.timestampType());
          System.out.format("topic: %s\n", record.topic());
          System.out.format("key: %s\n", record.key());
          System.out.format("value: %s\n", record.value());
        }

        consumer.commitAsync(
            new OffsetCommitCallback() {
              private final int marker = atomicInteger.incrementAndGet();

              @Override
              public void onComplete(
                  Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                if (exception != null) {
                  if (marker == atomicInteger.get()) consumer.commitAsync(this);
                } else {
                  // Can't try anymore
                }
              }
            });
      }
    } catch (WakeupException e) {
      // ignore for shutdown
    } finally {
      consumer.commitSync(); // Block
      consumer.close();
      System.out.println("Closed consumer and we are done");
    }
  }
}
