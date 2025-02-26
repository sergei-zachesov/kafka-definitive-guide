package ru.zachesov.lern.exactlyonce;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;

public class Transactional {

  public static void main(String[] args) {

    String transactionalId = "32131";
    String groupId = "group1";
    String inputTopic = "topic1";

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, "DemoProducer");
    producerProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
    var producer = new KafkaProducer<Integer, String>(producerProps);
    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    var consumer = new KafkaConsumer<Integer, String>(consumerProps);
    producer.initTransactions();
    consumer.subscribe(List.of(inputTopic));
    while (true) {
      try {
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(200));
        if (records.count() > 0) {
          producer.beginTransaction();
          for (ConsumerRecord<Integer, String> record : records) {
            ProducerRecord<Integer, String> customizedRecord = transform(record);
            producer.send(customizedRecord);
          }
          Map<TopicPartition, OffsetAndMetadata> offsets = consumerOffsets();
          producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
          producer.commitTransaction();
        }
      } catch (ProducerFencedException | InvalidProducerEpochException e) {
        throw new KafkaException(
            String.format("The transactional.id %s is used by another process", transactionalId));
      } catch (KafkaException e) {
        producer.abortTransaction();
        resetToLastCommittedPositions(consumer);
      }
    }
  }

  private static void resetToLastCommittedPositions(KafkaConsumer<Integer, String> consumer) {}

  private static Map<TopicPartition, OffsetAndMetadata> consumerOffsets() {
    return null;
  }

  private static ProducerRecord<Integer, String> transform(ConsumerRecord<Integer, String> record) {
    return null;
  }
}
