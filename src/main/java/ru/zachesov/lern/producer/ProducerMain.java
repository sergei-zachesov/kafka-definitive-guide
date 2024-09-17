package ru.zachesov.lern.producer;

import java.util.Properties;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerMain {

  public static void main(String[] args) {

    // Создание продюсера
    Properties kafkaProps = new Properties();
    kafkaProps.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092, broker2:9092");
    kafkaProps.put(CommonClientConfigs.CLIENT_ID_CONFIG, "myKafkaProducer");
    kafkaProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    kafkaProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    kafkaProps.put(ProducerConfig.ACKS_CONFIG, "1");
    kafkaProps.put(CommonClientConfigs.RETRIES_CONFIG, 1);
    kafkaProps.put(ProducerConfig.LINGER_MS_CONFIG, 10);
    kafkaProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33_554_432); // bytes
    kafkaProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384); // bytes

    KafkaProducer<String, String> producer = new KafkaProducer<>(kafkaProps);

    // Отправка
    ProducerRecord<String, String> record =
        new ProducerRecord<>("CustomerCountry", "Precision Products", "France");
    try {
      producer.send(record);
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Отправка асинхронная отправка
    producer.send(record, new DemoProducerCallback());

    record = new ProducerRecord<>("CustomerCountry", "France");
  }

  private static class DemoProducerCallback implements Callback {

    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
      if (e != null) {
        e.printStackTrace();
      }
    }
  }
}
