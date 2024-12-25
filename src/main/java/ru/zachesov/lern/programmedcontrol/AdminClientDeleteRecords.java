package ru.zachesov.lern.programmedcontrol;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class AdminClientDeleteRecords {

  private static final String CONSUMER_GROUP = "group1";

  @SneakyThrows
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    AdminClient admin = AdminClient.create(props);

    Map<TopicPartition, OffsetAndMetadata> offsets =
        admin.listConsumerGroupOffsets(CONSUMER_GROUP).partitionsToOffsetAndMetadata().get();

    Map<TopicPartition, OffsetSpec> requestOlderOffsets = new HashMap<>();
    for (TopicPartition tp : offsets.keySet()) {
      requestOlderOffsets.put(tp, OffsetSpec.earliest());
    }

    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> olderOffsets =
        admin.listOffsets(requestOlderOffsets).all().get();
    Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();
    for (Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e :
        olderOffsets.entrySet())
      recordsToDelete.put(e.getKey(), RecordsToDelete.beforeOffset(e.getValue().offset()));
    admin.deleteRecords(recordsToDelete).all().get();
  }
}
