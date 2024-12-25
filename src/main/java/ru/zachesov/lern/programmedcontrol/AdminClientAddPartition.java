package ru.zachesov.lern.programmedcontrol;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewPartitions;

public class AdminClientAddPartition {

  private static final String TOPIC_NAME = "topic1";
  private static final int NUM_PARTITIONS = 1;

  @SneakyThrows
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    AdminClient admin = AdminClient.create(props);

    Map<String, NewPartitions> newPartitions = new HashMap<>();
    newPartitions.put(TOPIC_NAME, NewPartitions.increaseTo(NUM_PARTITIONS + 2));
    admin.createPartitions(newPartitions).all().get();
  }
}
