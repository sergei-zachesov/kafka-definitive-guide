package ru.zachesov.lern.programmedcontrol;

import java.util.*;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;

public class AdminClientRedistributingReplicas {

  private static final List<String> TOPIC_LIST =
      List.of("topic1", "topic2", "topic3", "topic4", "topic5", "topic6");
  private static final String TOPIC_NAME = "topic1";

  @SneakyThrows
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    AdminClient admin = AdminClient.create(props);

    Map<TopicPartition, Optional<NewPartitionReassignment>> reassignment = new HashMap<>();
    reassignment.put(
        new TopicPartition(TOPIC_NAME, 0),
        Optional.of(new NewPartitionReassignment(List.of(0, 1))));
    reassignment.put(
        new TopicPartition(TOPIC_NAME, 1), Optional.of(new NewPartitionReassignment(List.of(1))));
    reassignment.put(
        new TopicPartition(TOPIC_NAME, 2),
        Optional.of(new NewPartitionReassignment(List.of(1, 0))));
    reassignment.put(new TopicPartition(TOPIC_NAME, 3), Optional.empty());
    admin.alterPartitionReassignments(reassignment).all().get();
    System.out.println(
        "currently reassigning: " + admin.listPartitionReassignments().reassignments().get());
    DescribeTopicsResult demoTopic = admin.describeTopics(TOPIC_LIST);
    TopicDescription topicDescription = demoTopic.topicNameValues().get(TOPIC_NAME).get();
    System.out.println("Description of demo topic:" + topicDescription);
  }
}
