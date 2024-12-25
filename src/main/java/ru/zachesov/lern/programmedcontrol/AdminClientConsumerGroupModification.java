package ru.zachesov.lern.programmedcontrol;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnknownMemberIdException;

public class AdminClientConsumerGroupModification {

  private static final String CONSUMER_GROUP = "group1";

  @SneakyThrows
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    AdminClient admin = AdminClient.create(props);

    Map<TopicPartition, OffsetAndMetadata> offsets =
        admin.listConsumerGroupOffsets(CONSUMER_GROUP).partitionsToOffsetAndMetadata().get();
    Map<TopicPartition, OffsetSpec> requestEarliestOffsets = new HashMap<>();
    for (TopicPartition tp : offsets.keySet()) {
      requestEarliestOffsets.put(tp, OffsetSpec.earliest());
    }

    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets =
        admin.listOffsets(requestEarliestOffsets).all().get();
    Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();
    for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e :
        earliestOffsets.entrySet()) {
      resetOffsets.put(e.getKey(), new OffsetAndMetadata(e.getValue().offset()));
    }
    try {
      admin.alterConsumerGroupOffsets(CONSUMER_GROUP, resetOffsets).all().get();
    } catch (ExecutionException e) {
      System.out.println(
          "Failed to update the offsets committed by group "
              + CONSUMER_GROUP
              + " with error "
              + e.getMessage());
      if (e.getCause() instanceof UnknownMemberIdException)
        System.out.println("Check if consumer group is still active.");
    }
  }
}
