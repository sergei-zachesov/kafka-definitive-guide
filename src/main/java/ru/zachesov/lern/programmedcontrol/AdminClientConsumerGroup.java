package ru.zachesov.lern.programmedcontrol;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

public class AdminClientConsumerGroup {

  private static final List<String> CONSUMER_GRP_LIST = List.of("group1", "group2");
  private static final String CONSUMER_GROUP = "group1";

  @SneakyThrows
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    AdminClient admin = AdminClient.create(props);

    // Группы
    admin.listConsumerGroups().valid().get().forEach(System.out::println);

    // Описание группы
    ConsumerGroupDescription groupDescription =
        admin.describeConsumerGroups(CONSUMER_GRP_LIST).describedGroups().get(CONSUMER_GROUP).get();
    System.out.println("Description of group " + CONSUMER_GROUP + ":" + groupDescription);

    // Отставания зафиксированного смещения от текущего сообщения
    Map<TopicPartition, OffsetAndMetadata> offsets =
        admin.listConsumerGroupOffsets(CONSUMER_GROUP).partitionsToOffsetAndMetadata().get();
    Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();
    for (TopicPartition tp : offsets.keySet()) {
      requestLatestOffsets.put(tp, OffsetSpec.latest());
    }

    Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
        admin.listOffsets(requestLatestOffsets).all().get();
    for (Map.Entry<TopicPartition, OffsetAndMetadata> e : offsets.entrySet()) {
      String topic = e.getKey().topic();
      int partition = e.getKey().partition();
      long committedOffset = e.getValue().offset();
      long latestOffset = latestOffsets.get(e.getKey()).offset();

      System.out.println(
          "Consumer group "
              + CONSUMER_GROUP
              + " has committed offset "
              + committedOffset
              + " to topic "
              + topic
              + " partition "
              + partition
              + ". The latest offset in the partition is "
              + latestOffset
              + " so consumer group is "
              + (latestOffset - committedOffset)
              + " records behind");
    }
  }
}
