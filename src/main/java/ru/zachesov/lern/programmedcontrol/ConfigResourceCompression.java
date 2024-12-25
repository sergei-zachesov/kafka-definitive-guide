package ru.zachesov.lern.programmedcontrol;

import java.util.*;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

public class ConfigResourceCompression {
  private static final String TOPIC_NAME = "topic1";

  @SneakyThrows
  public static void main(String[] args) {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    AdminClient admin = AdminClient.create(props);

    ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
    DescribeConfigsResult configsResult = admin.describeConfigs(List.of(configResource));
    Config configs = configsResult.all().get().get(configResource);
    // print nondefault configs
    configs.entries().stream().filter(entry -> !entry.isDefault()).forEach(System.out::println);
    // Check if topic is compacted
    ConfigEntry compaction =
        new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
    if (!configs.entries().contains(compaction)) {
      // if topic is not compacted, compact it
      Collection<AlterConfigOp> configOp = new ArrayList<>();
      configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET));
      Map<ConfigResource, Collection<AlterConfigOp>> alterConf = new HashMap<>();
      alterConf.put(configResource, configOp);
      admin.incrementalAlterConfigs(alterConf).all().get();
    } else {
      System.out.println("Topic " + TOPIC_NAME + " is compacted topic");
    }
  }
}
