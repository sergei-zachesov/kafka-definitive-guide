package ru.zachesov.lern.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class CustomerDeserializerAvro {
  public static void main(String[] args) {

    String schemaUrl = "url";

    Duration timeout = Duration.ofMillis(100);
    Properties props = new Properties();
    props.put("bootstrap.servers", "broker1:9092,broker2:9092");
    props.put("group.id", "CountryCounter");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer");
    props.put("specific.avro.reader", "true");
    props.put("schema.registry.url", schemaUrl);
    String topic = "customerContacts";
    KafkaConsumer<String, Customer> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topic));
    System.out.println("Reading topic:" + topic);
    while (true) {
      ConsumerRecords<String, Customer> records = consumer.poll(timeout);
      for (ConsumerRecord<String, Customer> record : records) {
        System.out.println("Current customer name is: " + record.value().getName());
      }
      consumer.commitSync();
    }
  }
}
