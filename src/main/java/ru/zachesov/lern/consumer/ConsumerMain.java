package ru.zachesov.lern.consumer;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.json.JSONObject;

@Slf4j
public class ConsumerMain {

  public static void main(String[] args) {

    Properties props = new Properties();
    props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092,broker2:9092");
    props.put(CommonClientConfigs.GROUP_ID_CONFIG, "CountryCounter");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 2);

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    consumer.subscribe(List.of("customerCountries"));

    Map<String, Integer> custCountryMap = new HashMap<>();
    Duration timeout = Duration.ofMillis(100);

    while (true) {
      ConsumerRecords<String, String> records = consumer.poll(timeout);

      for (ConsumerRecord<String, String> record : records) {
        System.out.printf(
            "topic = %s, partition = %d, offset = %d, " + "customer = %s, country = %s\n",
            record.topic(), record.partition(), record.offset(), record.key(), record.value());

        custCountryMap.merge(record.value(), 1, (oldValue, newValue) -> oldValue + 1);

        JSONObject json = new JSONObject(custCountryMap);
        System.out.println(json);
      }

      // Синхронная фиксация смещения после обработки всего пакета сообщений
      try {
        consumer.commitSync();
      } catch (CommitFailedException e) {
        log.error("commit failed", e);
      }

      // Асинхронная фиксация
      consumer.commitAsync();

      // Асинхронная фиксация с обратным вызовом
      consumer.commitAsync(
          (offsets, e) -> {
            if (e != null) log.error("Commit failed for offsets {}", offsets, e);
          });

    }
  }
}
