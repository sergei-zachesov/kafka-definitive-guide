package ru.zachesov.lern.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.Properties;
import org.apache.kafka.clients.producer.*;

public class ProducerAvro {

  public static void main(String[] args) {

    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", KafkaAvroSerializer.class);
    props.put("value.serializer", KafkaAvroSerializer.class);
    props.put("schema.registry.url", "localhost:9093");

    String topic = "customerContacts";
    Producer<String, Customer> producer = new KafkaProducer<>(props);
    // Генерация новых событий продолжается вплоть до нажатия Ctrl+C
    while (true) {
      Customer customer = new Customer(12, "Тест");
      System.out.println("Generated customer " + customer);
      ProducerRecord<String, Customer> record =
          new ProducerRecord<>(topic, customer.getName(), customer);
      producer.send(record);
    }
  }

  private static class Customer {
    private int customerID;
    private String customerName;

    public Customer(int ID, String name) {
      this.customerID = ID;
      this.customerName = name;
    }

    public int getID() {
      return customerID;
    }

    public String getName() {
      return customerName;
    }
  }
}
