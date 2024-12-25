package ru.zachesov.lern.consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class ConsumerSetTopic {

  public static void main(String[] args) {
    Properties props = new Properties();
    props.put("bootstrap.servers", "broker1:9092,broker2:9092");
    props.put("group.id", "CountryCounter");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringDeserializer");
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

    Duration timeout = Duration.ofMillis(100);
    List<PartitionInfo> partitionInfos = consumer.partitionsFor("topic");
    List<TopicPartition> partitions = new ArrayList<>();
    if (partitionInfos != null) {
      for (PartitionInfo partition : partitionInfos)
        partitions.add(new TopicPartition(partition.topic(), partition.partition()));
      consumer.assign(partitions);
      while (true) {
        ConsumerRecords<String, String> records = consumer.poll(timeout);
        for (ConsumerRecord<String, String> record : records) {
          System.out.printf(
              "topic = %s, partition = %s, offset = %d, customer = %s, country = %s\n",
              record.topic(), record.partition(), record.offset(), record.key(), record.value());
        }
        consumer.commitSync();
      }
    }
  }
}
