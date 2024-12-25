package ru.zachesov.lern.programmedcontrol;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.ElectionType;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ElectionNotNeededException;

public class AdminClientNewLeader {

  private static final String TOPIC_NAME = "topic1";

  @SneakyThrows
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    AdminClient admin = AdminClient.create(props);

    Set<TopicPartition> electableTopics = new HashSet<>();
    electableTopics.add(new TopicPartition(TOPIC_NAME, 0));
    try {
      admin.electLeaders(ElectionType.PREFERRED, electableTopics).all().get();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof ElectionNotNeededException) {
        System.out.println("All leaders are preferred already");
      }
    }
  }
}
